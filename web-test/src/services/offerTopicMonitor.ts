import type { Admin, Kafka } from "kafkajs";
import { getAppConfig } from "../config.js";

const PARTITION = 0;

export type OfferTopicEndOffsetSnapshot = {
  topic: string;
  partition: number;
  /** High watermark from `fetchTopicOffsets` on each request. */
  end_offset: string | null;
  /** Unused (reserved); always `null`. */
  latest_consumed_offset: string | null;
  /** Unused (reserved); always `null`. */
  latest_message_preview: string | null;
  end_offset_queried_at_ms: number | null;
  /** Unused (reserved); always `null`. */
  latest_consumed_at_ms: number | null;
  /** Whether {@link start} finished (admin pre-connect). */
  running: boolean;
  last_error: string | null;
};

/**
 * Queries `offer.<market>` partition 0 **high watermark** via Kafka Admin on each {@link getSnapshot} call.
 * Does not subscribe or read message payloads.
 */
export class OfferTopicMonitor {
  private admin: Admin | null = null;
  private adminConnect: Promise<void> | null = null;
  private started = false;

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
      latest_consumed_offset: null,
      latest_message_preview: null,
      end_offset_queried_at_ms,
      latest_consumed_at_ms: null,
      running: this.started,
      last_error: brokerErr,
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
    try {
      await this.ensureAdminConnected();
      this.started = true;
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error("[offer-topic-monitor] admin connect failed", e);
      throw new Error(msg);
    }
  }

  async stop(): Promise<void> {
    this.started = false;
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
