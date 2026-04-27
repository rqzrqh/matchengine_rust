import type { Admin, Consumer, Kafka } from "kafkajs";
import { getAppConfig } from "../config.js";

const PARTITION = 0;
const PREVIEW_MAX = 12_000;

export type OfferTopicEndOffsetSnapshot = {
  topic: string;
  partition: number;
  /** Log end high watermark (queried from broker on each HTTP request to `getSnapshot`). */
  end_offset: string | null;
  /**
   * Latest consumed offer message offset, updated in `eachMessage` (new writes only, no history replay).
   * Compare to {@link end_offset} to estimate consumer lag.
   */
  latest_consumed_offset: string | null;
  /** Truncated preview of the message at {@link latest_consumed_offset}. */
  latest_message_preview: string | null;
  end_offset_queried_at_ms: number | null;
  latest_consumed_at_ms: number | null;
  running: boolean;
  last_error: string | null;
};

/**
 * Kafka **consumer** on `offer.<market>`: after start, **seek** to **latest** and only consume new offers;
 * each message updates in-memory **latest_consumed_offset** and preview.
 * **end_offset** (partition high) is read from the broker only inside `getSnapshot()` (HTTP), not on a background timer.
 */
export class OfferTopicMonitor {
  private consumer: Consumer | null = null;
  private admin: Admin | null = null;
  private adminConnect: Promise<void> | null = null;
  private started = false;

  /** String form of the last processed record offset (same as `message.offset`). */
  private latestConsumedOffset: string | null = null;
  private latestPreview: string | null = null;
  private latestConsumedAt: number | null = null;
  private lastConsumerError: string | null = null;

  constructor(private readonly kafka: Kafka) {}

  async getSnapshot(): Promise<OfferTopicEndOffsetSnapshot> {
    const cfg = getAppConfig();
    const topic = `offer.${cfg.marketName}`;

    let end_offset: string | null = null;
    let end_offset_queried_at_ms: number | null = null;
    let brokerErr: string | null = null;
    try {
      await this.ensureAdminConnected();
      const offs = await this.admin!.fetchTopicOffsets(topic);
      const p0 = offs.find((x) => x.partition === PARTITION);
      if (p0) {
        const raw = p0.high ?? p0.offset;
        if (raw !== undefined && raw !== null && String(raw).trim() !== "") {
          end_offset = String(raw);
          end_offset_queried_at_ms = Date.now();
        } else {
          brokerErr = "no high/offset in fetchTopicOffsets";
        }
      } else {
        brokerErr = `partition ${PARTITION} not found`;
      }
    } catch (e) {
      brokerErr = e instanceof Error ? e.message : String(e);
    }

    return {
      topic,
      partition: PARTITION,
      end_offset,
      latest_consumed_offset: this.latestConsumedOffset,
      latest_message_preview: this.latestPreview,
      end_offset_queried_at_ms,
      latest_consumed_at_ms: this.latestConsumedAt,
      running: this.started,
      last_error: brokerErr ?? this.lastConsumerError,
    };
  }

  private async ensureAdminConnected(): Promise<void> {
    if (!this.admin) {
      this.admin = this.kafka.admin();
      this.adminConnect = this.admin.connect();
    }
    await this.adminConnect;
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    const cfg = getAppConfig();
    const topic = `offer.${cfg.marketName}`;

    const groupId = `web-offer-topic-tail-${cfg.marketName}`;
    this.consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 30_000,
      heartbeatInterval: 3_000,
    });

    let initialSeekDone = false;
    const onGroupJoin = async () => {
      if (initialSeekDone || !this.consumer) return;
      initialSeekDone = true;
      try {
        // Same as quote service: offset "-1" = tail the partition, only messages written after this consumer starts.
        await this.consumer.seek({ topic, partition: PARTITION, offset: "-1" });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        console.error("[offer-topic-monitor] seek to latest failed", e);
        this.lastConsumerError = msg;
      }
    };
    this.consumer.on(this.consumer.events.GROUP_JOIN, onGroupJoin);

    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [topic], fromBeginning: true });

    void this.consumer.run({
      eachMessage: async ({ message, topic: t }) => {
        this.lastConsumerError = null;
        const off = message.offset;
        this.latestConsumedOffset = off;
        this.latestConsumedAt = Date.now();
        const raw = message.value?.toString() ?? "";
        this.latestPreview =
          raw.length > PREVIEW_MAX ? `${raw.slice(0, PREVIEW_MAX)}…` : raw || null;
        void t;
      },
    });
  }

  async stop(): Promise<void> {
    this.started = false;
    try {
      if (this.consumer) {
        await this.consumer.disconnect();
      }
    } catch {
      /* ignore */
    }
    this.consumer = null;
    try {
      if (this.admin) {
        await this.admin.disconnect();
      }
    } catch {
      /* ignore */
    }
    this.admin = null;
    this.adminConnect = null;
  }
}
