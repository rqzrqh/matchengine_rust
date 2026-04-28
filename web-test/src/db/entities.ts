import {
  Column,
  Entity,
  Index,
  PrimaryColumn,
  PrimaryGeneratedColumn,
  Unique,
} from "typeorm";

/** Explicit MySQL column types — required under `tsx`/ESM where `emitDecoratorMetadata` does not run. */

@Entity("quote_deal_ticks")
@Index("idx_quote_deal_ticks_time", ["time"])
export class QuoteDealTick {
  @PrimaryGeneratedColumn({ type: "int" })
  id!: number;

  @Column({ name: "ingested_at", type: "int" })
  ingestedAt!: number;

  @Column({ name: "kafka_topic", type: "varchar", length: 191 })
  kafkaTopic!: string;

  @Column({ type: "varchar", length: 191 })
  market!: string;

  @Column({ name: "deal_id", type: "int", nullable: true })
  dealId!: number | null;

  @Column({ type: "int", nullable: true })
  time!: number | null;

  @Column({ type: "int", nullable: true })
  side!: number | null;

  @Column({ type: "varchar", length: 191 })
  price!: string;

  @Column({ type: "varchar", length: 191 })
  amount!: string;

  @Column({ name: "raw_json", type: "text" })
  rawJson!: string;
}

@Entity("quote_consumer_state")
@Unique("uq_quote_consumer_state_topic_part", ["kafkaTopic", "partitionId"])
export class QuoteConsumerState {
  @PrimaryGeneratedColumn({ type: "int" })
  id!: number;

  @Column({ name: "kafka_topic", type: "varchar", length: 191 })
  kafkaTopic!: string;

  @Column({ name: "partition_id", type: "int", default: 0 })
  partitionId!: number;

  @Column({ name: "last_offset", type: "bigint" })
  lastOffset!: string;

  @Column({ name: "last_deal_id", type: "int", nullable: true })
  lastDealId!: number | null;

  @Column({ name: "updated_at", type: "int" })
  updatedAt!: number;
}

@Entity("settle_messages")
@Index("idx_settle_messages_ingested", ["ingestedAt"])
export class SettleMessage {
  @PrimaryGeneratedColumn({ type: "int" })
  id!: number;

  @Column({ name: "ingested_at", type: "int" })
  ingestedAt!: number;

  @Column({ name: "kafka_topic", type: "varchar", length: 191 })
  kafkaTopic!: string;

  @Column({ name: "raw_json", type: "text" })
  rawJson!: string;
}

@Entity("settle_consumer_state")
@Unique("uq_settle_consumer_state_group_mkt", ["settleGroupId", "market"])
export class SettleConsumerState {
  @PrimaryGeneratedColumn({ type: "int" })
  id!: number;

  @Column({ name: "settle_group_id", type: "int" })
  settleGroupId!: number;

  @Column({ type: "varchar", length: 191 })
  market!: string;

  @Column({ name: "last_offset", type: "bigint" })
  lastOffset!: string;

  @Column({ name: "last_msgid", type: "int", nullable: true })
  lastMsgid!: number | null;

  @Column({ name: "updated_at", type: "int" })
  updatedAt!: number;
}

@Entity("offer_orders")
@Index("idx_offer_orders_time", ["createdAt"])
@Index("idx_offer_orders_sequence", ["sequenceId"])
export class OfferOrder {
  @PrimaryColumn({ type: "int" })
  id!: number;

  @Column({ name: "sequence_id", type: "int", nullable: true })
  sequenceId!: number | null;

  @Column({ name: "created_at", type: "int" })
  createdAt!: number;

  @Column({ type: "varchar", length: 64 })
  kind!: string;

  @Column({ name: "extern_id", type: "int" })
  externId!: number;

  @Column({ name: "user_id", type: "int" })
  userId!: number;

  @Column({ type: "varchar", length: 512 })
  summary!: string;

  @Column({ type: "varchar", length: 32 })
  status!: string;

  @Column({ type: "varchar", length: 1024, nullable: true })
  error!: string | null;

  @Column({ name: "payload_json", type: "text" })
  payloadJson!: string;
}

@Entity("offer_dispatch_state")
export class OfferDispatchState {
  @PrimaryColumn({ type: "int" })
  id!: number;

  @Column({ name: "last_sequence_id", type: "int", default: 0 })
  lastSequenceId!: number;

  @Column({ name: "last_sent_sequence_id", type: "int", default: 0 })
  lastSentSequenceId!: number;

  @Column({ name: "updated_at", type: "int" })
  updatedAt!: number;
}
