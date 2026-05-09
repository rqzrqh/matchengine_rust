import { Kafka, type Consumer } from "kafkajs";
import { getAppConfig } from "../config.js";
import type { AppDb } from "../db.js";
import { getDb } from "../db.js";
import { SettleConsumerState, SettleMessage } from "../db/entities.js";
import { unixSecondsNow } from "../time.js";
import type { FeedHub } from "./feedHub.js";

const SETTLE_TOPIC = "settle";

/**
 * Consumes Kafka topic `settle` (N partitions, default 64); `settle_group_id` = message partition index.
 * Seeks each partition on startup. For each partition, a message is accepted only
 * when `settle_message_id == db_last_settle_message_id + 1`; stale/duplicate ids
 * only advance the offset, and gaps raise an error.
 */
export class SettleService {
  private consumer: Consumer | null = null;

  constructor(
    private readonly kafka: Kafka,
    private readonly hub: FeedHub,
  ) {}

  async start(): Promise<void> {
    const cfg = getAppConfig();
    const marketName = cfg.marketName;
    const groupCount = cfg.userSettleGroupSize;
    const groupId = `web-settle-messages-${marketName}`;

    this.consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxWaitTimeInMs: 5000,
    });

    const mgr = getDb().manager;
    const states = await mgr.find(SettleConsumerState, {
      where: { market: marketName },
    });

    const byGroup = new Map(states.map((s) => [s.settleGroupId, s]));

    let initialSeekDone = false;
    const onGroupJoin = async () => {
      if (initialSeekDone || !this.consumer) return;
      initialSeekDone = true;
      try {
        for (let i = 0; i < groupCount; i++) {
          const st = byGroup.get(i);
          if (st) {
            const next = (BigInt(st.lastOffset) + 1n).toString();
            await this.consumer.seek({ topic: SETTLE_TOPIC, partition: i, offset: next });
          } else if (cfg.kafkaAutoOffsetReset !== "earliest") {
            await this.consumer.seek({ topic: SETTLE_TOPIC, partition: i, offset: "-1" });
          }
        }
      } catch (e) {
        console.error("[settle] initial seek failed", e);
      }
    };
    this.consumer.on(this.consumer.events.GROUP_JOIN, onGroupJoin);

    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [SETTLE_TOPIC], fromBeginning: true });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        if (!this.consumer) return;
        const offset = BigInt(message.offset);
        if (topic !== SETTLE_TOPIC) {
          await this.safeCommit(topic, partition, offset);
          return;
        }
        const settleGroup = partition;

        const raw = message.value?.toString() ?? "{}";
        let payload: Record<string, unknown>;
        try {
          payload = JSON.parse(raw) as Record<string, unknown>;
        } catch {
          await this.handleBadJson(getDb(), topic, settleGroup, marketName, offset);
          await this.safeCommit(topic, partition, offset);
          return;
        }

        const market = String(payload.market ?? marketName);
        const settleMessageId = u64OrNull(payload.settle_message_id);

        const inserted = await this.handleSettleMessage(
          getDb(),
          topic,
          settleGroup,
          market,
          offset,
          raw,
          settleMessageId,
        );

        if (inserted) {
          this.hub.broadcast("settle", { topic, ...payload });
        }

        await this.safeCommit(topic, partition, offset);
      },
    });
  }

  private async safeCommit(topic: string, partition: number, offset: bigint): Promise<void> {
    if (!this.consumer) return;
    const next = (offset + 1n).toString();
    await this.consumer.commitOffsets([{ topic, partition, offset: next }]);
  }

  private async handleBadJson(
    ds: AppDb,
    topic: string,
    settleGroup: number,
    market: string,
    offset: bigint,
  ): Promise<void> {
    const mgr = ds.manager;
    const row = await mgr.findOne(SettleConsumerState, {
      where: { settleGroupId: settleGroup, market },
    });
    await this.upsertOffsetOnly(ds, settleGroup, market, offset, row?.lastSettleMessageId ?? null);
  }

  /**
   * @returns whether a new `settle_messages` row was inserted (triggers WebSocket push)
   */
  private async handleSettleMessage(
    ds: AppDb,
    topic: string,
    settleGroup: number,
    market: string,
    offset: bigint,
    raw: string,
    settleMessageId: bigint | null,
  ): Promise<boolean> {
    const mgr = ds.manager;
    const row = await mgr.findOne(SettleConsumerState, {
      where: { settleGroupId: settleGroup, market },
    });

    const lastSettleMessageId = parseStoredU64(row?.lastSettleMessageId);

    if (settleMessageId === null) {
      throw new Error(
        `[settle] invalid settle_message_id topic=${topic} partition=${settleGroup} offset=${offset} market=${market}`,
      );
    }

    if (settleMessageId <= lastSettleMessageId) {
      await this.upsertOffsetOnly(
        ds,
        settleGroup,
        market,
        offset,
        row?.lastSettleMessageId ?? lastSettleMessageId.toString(),
      );
      return false;
    }

    const expected = lastSettleMessageId + 1n;
    if (settleMessageId !== expected) {
      throw new Error(
        `[settle] settle_message_id gap topic=${topic} partition=${settleGroup} offset=${offset} market=${market} message=${settleMessageId} expected=${expected} current=${lastSettleMessageId}`,
      );
    }

    const now = unixSecondsNow();

    await mgr.insert(SettleMessage, {
      ingestedAt: now,
      kafkaTopic: topic,
      rawJson: raw,
    });

    await this.upsertWithSettleMessageId(ds, settleGroup, market, offset, settleMessageId, now);
    return true;
  }

  private async upsertOffsetOnly(
    ds: AppDb,
    settleGroup: number,
    market: string,
    offset: bigint,
    keepSettleMessageId: string | null,
  ): Promise<void> {
    const mgr = ds.manager;
    const now = unixSecondsNow();
    const r = await mgr.findOne(SettleConsumerState, {
      where: { settleGroupId: settleGroup, market },
    });

    if (r) {
      await mgr.update(
        SettleConsumerState,
        { id: r.id },
        { lastOffset: offset.toString(), updatedAt: now },
      );
    } else {
      await mgr.insert(SettleConsumerState, {
        settleGroupId: settleGroup,
        market,
        lastOffset: offset.toString(),
        lastSettleMessageId: keepSettleMessageId,
        updatedAt: now,
      });
    }
  }

  private async upsertWithSettleMessageId(
    ds: AppDb,
    settleGroup: number,
    market: string,
    offset: bigint,
    settleMessageId: bigint,
    now: number,
  ): Promise<void> {
    const mgr = ds.manager;
    const r = await mgr.findOne(SettleConsumerState, {
      where: { settleGroupId: settleGroup, market },
    });

    if (r) {
      await mgr.update(
        SettleConsumerState,
        { id: r.id },
        {
          lastOffset: offset.toString(),
          lastSettleMessageId: settleMessageId.toString(),
          updatedAt: now,
        },
      );
    } else {
      await mgr.insert(SettleConsumerState, {
        settleGroupId: settleGroup,
        market,
        lastOffset: offset.toString(),
        lastSettleMessageId: settleMessageId.toString(),
        updatedAt: now,
      });
    }
  }

  async stop(): Promise<void> {
    if (this.consumer) {
      await this.consumer.disconnect();
      this.consumer = null;
    }
  }
}

function parseStoredU64(v: string | null | undefined): bigint {
  if (!v) return 0n;
  return BigInt(v);
}

function u64OrNull(v: unknown): bigint | null {
  if (v === undefined || v === null) return null;
  if (typeof v === "bigint") return v >= 0n ? v : null;
  if (typeof v === "number") {
    if (!Number.isSafeInteger(v) || v < 0) return null;
    return BigInt(v);
  }
  if (typeof v === "string" && /^[0-9]+$/.test(v)) {
    return BigInt(v);
  }
  return null;
}

export function serializeSettleMessage(r: SettleMessage): Record<string, unknown> {
  return {
    id: r.id,
    ingested_at: r.ingestedAt,
    kafka_topic: r.kafkaTopic,
    raw_json: r.rawJson,
  };
}
