import { Kafka, type Consumer } from "kafkajs";
import { getAppConfig } from "../config.js";
import type { AppDb } from "../db.js";
import { getDb } from "../db.js";
import { QuoteConsumerState, QuoteDealTick } from "../db/entities.js";
import { unixSecondsNow } from "../time.js";
import type { FeedHub } from "./feedHub.js";

const PARTITION = 0;

/**
 * Consumes Kafka `quote_deals.<market>`, resumes from `quote_consumer_state`, and writes `quote_deal_ticks`.
 * If `info.deal_id` lags `last_deal_id`, only the offset advances; otherwise a tick row is inserted and offset/deal_id update.
 */
export class QuoteService {
  private consumer: Consumer | null = null;

  constructor(
    private readonly kafka: Kafka,
    private readonly hub: FeedHub,
  ) {}

  async start(): Promise<void> {
    const cfg = getAppConfig();
    const quoteTopic = `quote_deals.${cfg.marketName}`;
    const groupId = `web-quote-deals-${cfg.marketName}`;

    this.consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxWaitTimeInMs: 5000,
    });

    const mgr = getDb().manager;
    const st = await mgr.findOne(QuoteConsumerState, {
      where: { kafkaTopic: quoteTopic, partitionId: PARTITION },
    });

    let initialSeekDone = false;
    const onGroupJoin = async () => {
      if (initialSeekDone || !this.consumer) return;
      initialSeekDone = true;
      try {
        if (st) {
          const next = (BigInt(st.lastOffset) + 1n).toString();
          await this.consumer.seek({ topic: quoteTopic, partition: PARTITION, offset: next });
        } else if (cfg.kafkaAutoOffsetReset !== "earliest") {
          await this.consumer.seek({ topic: quoteTopic, partition: PARTITION, offset: "-1" });
        }
      } catch (e) {
        console.error("[quote] initial seek failed", e);
      }
    };
    this.consumer.on(this.consumer.events.GROUP_JOIN, onGroupJoin);

    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [quoteTopic], fromBeginning: true });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        if (!this.consumer) return;
        const raw = message.value?.toString() ?? "{}";
        const offset = BigInt(message.offset);

        let payload: Record<string, unknown>;
        try {
          payload = JSON.parse(raw) as Record<string, unknown>;
        } catch {
          await this.handleBadJson(getDb(), topic, partition, offset);
          await this.safeCommit(topic, partition, offset);
          return;
        }

        const info = (payload.info ?? {}) as Record<string, unknown>;
        const market = String(payload.market ?? "");
        const dealId = parseDealId(info.deal_id);
        const time = typeof info.time === "number" ? info.time : Number(info.time) || null;
        const side = typeof info.side === "number" ? info.side : Number(info.side) || null;
        const price = String(info.price ?? "");
        const amount = String(info.amount ?? "");

        const inserted = await this.handleQuoteMessage(
          getDb(),
          topic,
          partition,
          offset,
          raw,
          market,
          dealId,
          time,
          side,
          price,
          amount,
        );

        if (inserted) {
          this.hub.broadcast("quote", { topic, ...payload });
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

  private async handleBadJson(ds: AppDb, topic: string, partition: number, offset: bigint): Promise<void> {
    const mgr = ds.manager;
    const row = await mgr.findOne(QuoteConsumerState, {
      where: { kafkaTopic: topic, partitionId: partition },
    });
    await this.upsertOffsetOnly(ds, topic, partition, offset, row?.lastDealId ?? null);
  }

  /**
   * @returns whether a new `quote_deal_ticks` row was inserted (triggers WebSocket push)
   */
  private async handleQuoteMessage(
    ds: AppDb,
    topic: string,
    partition: number,
    offset: bigint,
    raw: string,
    market: string,
    dealId: number | null,
    time: number | null,
    side: number | null,
    price: string,
    amount: string,
  ): Promise<boolean> {
    const mgr = ds.manager;
    const row = await mgr.findOne(QuoteConsumerState, {
      where: { kafkaTopic: topic, partitionId: partition },
    });

    const lastDealId = row?.lastDealId ?? null;

    if (dealId === null) {
      await this.upsertOffsetOnly(ds, topic, partition, offset, lastDealId);
      return false;
    }

    if (lastDealId !== null && dealId < lastDealId) {
      await this.upsertOffsetOnly(ds, topic, partition, offset, lastDealId);
      return false;
    }

    if (lastDealId !== null && dealId === lastDealId) {
      await this.upsertOffsetOnly(ds, topic, partition, offset, lastDealId);
      return false;
    }

    const now = unixSecondsNow();
    await mgr.insert(QuoteDealTick, {
      ingestedAt: now,
      kafkaTopic: topic,
      market,
      dealId,
      time,
      side,
      price,
      amount,
      rawJson: raw,
    });

    await this.upsertWithDeal(ds, topic, partition, offset, dealId, now);
    return true;
  }

  private async upsertOffsetOnly(
    ds: AppDb,
    topic: string,
    partition: number,
    offset: bigint,
    keepDealId: number | null,
  ): Promise<void> {
    const mgr = ds.manager;
    const now = unixSecondsNow();
    const row = await mgr.findOne(QuoteConsumerState, {
      where: { kafkaTopic: topic, partitionId: partition },
    });

    if (row) {
      await mgr.update(
        QuoteConsumerState,
        { id: row.id },
        { lastOffset: offset.toString(), updatedAt: now },
      );
    } else {
      await mgr.insert(QuoteConsumerState, {
        kafkaTopic: topic,
        partitionId: partition,
        lastOffset: offset.toString(),
        lastDealId: keepDealId,
        updatedAt: now,
      });
    }
  }

  private async upsertWithDeal(
    ds: AppDb,
    topic: string,
    partition: number,
    offset: bigint,
    dealId: number,
    now: number,
  ): Promise<void> {
    const mgr = ds.manager;
    const row = await mgr.findOne(QuoteConsumerState, {
      where: { kafkaTopic: topic, partitionId: partition },
    });

    if (row) {
      await mgr.update(
        QuoteConsumerState,
        { id: row.id },
        { lastOffset: offset.toString(), lastDealId: dealId, updatedAt: now },
      );
    } else {
      await mgr.insert(QuoteConsumerState, {
        kafkaTopic: topic,
        partitionId: partition,
        lastOffset: offset.toString(),
        lastDealId: dealId,
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

function parseDealId(v: unknown): number | null {
  if (v === undefined || v === null) return null;
  if (typeof v === "number" && Number.isFinite(v)) return Math.trunc(v);
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

export function serializeQuoteDealTick(r: QuoteDealTick): Record<string, unknown> {
  return {
    id: r.id,
    ingested_at: r.ingestedAt,
    kafka_topic: r.kafkaTopic,
    market: r.market,
    deal_id: r.dealId,
    time: r.time,
    side: r.side,
    price: r.price,
    amount: r.amount,
    raw_json: r.rawJson,
  };
}
