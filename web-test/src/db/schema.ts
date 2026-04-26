import { bigint, index, int, mysqlTable, text, uniqueIndex, varchar } from "drizzle-orm/mysql-core";

export const quoteDealTicks = mysqlTable(
  "quote_deal_ticks",
  {
    id: int("id").autoincrement().primaryKey(),
    ingestedAt: int("ingested_at").notNull(),
    kafkaTopic: varchar("kafka_topic", { length: 191 }).notNull(),
    market: varchar("market", { length: 191 }).notNull(),
    dealId: int("deal_id"),
    time: int("time"),
    side: int("side"),
    price: varchar("price", { length: 191 }).notNull(),
    amount: varchar("amount", { length: 191 }).notNull(),
    rawJson: text("raw_json").notNull(),
  },
  (t) => [index("idx_quote_deal_ticks_time").on(t.time)],
);

/** Quote consumer progress: partition offset + max ingested deal_id (Kafka `info.deal_id`) */
export const quoteConsumerState = mysqlTable(
  "quote_consumer_state",
  {
    id: int("id").autoincrement().primaryKey(),
    kafkaTopic: varchar("kafka_topic", { length: 191 }).notNull(),
    partitionId: int("partition_id").notNull().default(0),
    lastOffset: bigint("last_offset", { mode: "bigint" }).notNull(),
    lastDealId: int("last_deal_id"),
    updatedAt: int("updated_at").notNull(),
  },
  (t) => [uniqueIndex("uq_quote_consumer_state_topic_part").on(t.kafkaTopic, t.partitionId)],
);

/** Raw Kafka payload for settle topics (`settle.*`), one row per consumed message that advances `msgid`. */
export const settleMessages = mysqlTable(
  "settle_messages",
  {
    id: int("id").autoincrement().primaryKey(),
    ingestedAt: int("ingested_at").notNull(),
    kafkaTopic: varchar("kafka_topic", { length: 191 }).notNull(),
    rawJson: text("raw_json").notNull(),
  },
  (t) => [index("idx_settle_messages_ingested").on(t.ingestedAt)],
);

/** Settle consumer progress: one row per (settle group, market); `last_msgid` = Kafka root `msgid` */
export const settleConsumerState = mysqlTable(
  "settle_consumer_state",
  {
    id: int("id").autoincrement().primaryKey(),
    settleGroupId: int("settle_group_id").notNull(),
    market: varchar("market", { length: 191 }).notNull(),
    lastOffset: bigint("last_offset", { mode: "bigint" }).notNull(),
    lastMsgid: int("last_msgid"),
    updatedAt: int("updated_at").notNull(),
  },
  (t) => [uniqueIndex("uq_settle_consumer_state_group_mkt").on(t.settleGroupId, t.market)],
);

export const offerOrders = mysqlTable(
  "offer_orders",
  {
    id: int("id").primaryKey(),
    createdAt: int("created_at").notNull(),
    kind: varchar("kind", { length: 64 }).notNull(),
    externId: int("extern_id").notNull(),
    userId: int("user_id").notNull(),
    summary: varchar("summary", { length: 512 }).notNull(),
    status: varchar("status", { length: 32 }).notNull(),
    error: varchar("error", { length: 1024 }),
    payloadJson: text("payload_json").notNull(),
  },
  (t) => [index("idx_offer_orders_time").on(t.createdAt)],
);

/** Single row `id = 1`: last offer_orders.id successfully pushed to Kafka (inclusive). */
export const offerDispatchState = mysqlTable("offer_dispatch_state", {
  id: int("id").primaryKey(),
  lastSentOrderId: int("last_sent_order_id").notNull().default(0),
  updatedAt: int("updated_at").notNull(),
});

export type QuoteDealTick = typeof quoteDealTicks.$inferSelect;
export type QuoteConsumerStateRow = typeof quoteConsumerState.$inferSelect;
export type SettleMessageRow = typeof settleMessages.$inferSelect;
export type SettleConsumerStateRow = typeof settleConsumerState.$inferSelect;
export type OfferOrder = typeof offerOrders.$inferSelect;
export type OfferDispatchStateRow = typeof offerDispatchState.$inferSelect;
