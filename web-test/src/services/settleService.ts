import { and, eq } from "drizzle-orm";
import { Kafka, type Consumer } from "kafkajs";
import { getAppConfig } from "../config.js";
import type { AppDb } from "../db.js";
import { getDb } from "../db.js";
import { unixSecondsNow } from "../time.js";
import { settleConsumerState, settleMessages, type SettleMessageRow } from "../db/schema.js";
import type { FeedHub } from "./feedHub.js";

const SETTLE_TOPIC = "settle";

/**
 * Consumes Kafka topic `settle` (N partitions, default 64); `settle_group_id` = message partition index.
 * Seeks each partition on startup. When `msgid` moves forward, appends the raw payload to `settle_messages` and updates state; otherwise only the offset advances.
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

    const db = getDb();
    const states = await db
      .select()
      .from(settleConsumerState)
      .where(eq(settleConsumerState.market, marketName));

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
        const rootMsgid = numOrNull(payload.msgid);

        await this.handleSettleMessage(
          getDb(),
          topic,
          settleGroup,
          market,
          offset,
          raw,
          rootMsgid,
        );

        this.hub.broadcast("settle", { topic, ...payload });

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
    db: AppDb,
    topic: string,
    settleGroup: number,
    market: string,
    offset: bigint,
  ): Promise<void> {
    const [row] = await db
      .select()
      .from(settleConsumerState)
      .where(
        and(eq(settleConsumerState.settleGroupId, settleGroup), eq(settleConsumerState.market, market)),
      )
      .limit(1);
    await this.upsertOffsetOnly(db, settleGroup, market, offset, row?.lastMsgid ?? null);
  }

  /** Appends raw Kafka value to `settle_messages` when `msgid` advances. */
  private async handleSettleMessage(
    db: AppDb,
    topic: string,
    settleGroup: number,
    market: string,
    offset: bigint,
    raw: string,
    rootMsgid: number | null,
  ): Promise<void> {
    const [row] = await db
      .select()
      .from(settleConsumerState)
      .where(
        and(eq(settleConsumerState.settleGroupId, settleGroup), eq(settleConsumerState.market, market)),
      )
      .limit(1);

    const lastMsgid = row?.lastMsgid ?? null;

    if (rootMsgid === null) {
      await this.upsertOffsetOnly(db, settleGroup, market, offset, lastMsgid);
      return;
    }

    if (lastMsgid !== null && rootMsgid < lastMsgid) {
      await this.upsertOffsetOnly(db, settleGroup, market, offset, lastMsgid);
      return;
    }

    if (lastMsgid !== null && rootMsgid === lastMsgid) {
      await this.upsertOffsetOnly(db, settleGroup, market, offset, lastMsgid);
      return;
    }

    const now = unixSecondsNow();

    await db.insert(settleMessages).values({
      ingestedAt: now,
      kafkaTopic: topic,
      rawJson: raw,
    });

    await this.upsertWithMsgid(db, settleGroup, market, offset, rootMsgid, now);
  }

  private async upsertOffsetOnly(
    db: AppDb,
    settleGroup: number,
    market: string,
    offset: bigint,
    keepMsgid: number | null,
  ): Promise<void> {
    const now = unixSecondsNow();
    const [r] = await db
      .select()
      .from(settleConsumerState)
      .where(
        and(eq(settleConsumerState.settleGroupId, settleGroup), eq(settleConsumerState.market, market)),
      )
      .limit(1);

    if (r) {
      await db
        .update(settleConsumerState)
        .set({ lastOffset: offset, updatedAt: now })
        .where(eq(settleConsumerState.id, r.id));
    } else {
      await db.insert(settleConsumerState).values({
        settleGroupId: settleGroup,
        market,
        lastOffset: offset,
        lastMsgid: keepMsgid,
        updatedAt: now,
      });
    }
  }

  private async upsertWithMsgid(
    db: AppDb,
    settleGroup: number,
    market: string,
    offset: bigint,
    msgid: number,
    now: number,
  ): Promise<void> {
    const [r] = await db
      .select()
      .from(settleConsumerState)
      .where(
        and(eq(settleConsumerState.settleGroupId, settleGroup), eq(settleConsumerState.market, market)),
      )
      .limit(1);

    if (r) {
      await db
        .update(settleConsumerState)
        .set({ lastOffset: offset, lastMsgid: msgid, updatedAt: now })
        .where(eq(settleConsumerState.id, r.id));
    } else {
      await db.insert(settleConsumerState).values({
        settleGroupId: settleGroup,
        market,
        lastOffset: offset,
        lastMsgid: msgid,
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

function numOrNull(v: unknown): number | null {
  if (v === undefined || v === null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

export function serializeSettleMessage(r: SettleMessageRow): Record<string, unknown> {
  return {
    id: r.id,
    ingested_at: r.ingestedAt,
    kafka_topic: r.kafkaTopic,
    raw_json: r.rawJson,
  };
}
