import type { Producer } from "kafkajs";
import { In } from "typeorm";
import { getAppConfig } from "../config.js";
import { getDb } from "../db.js";
import { OfferDispatchState, OfferOrder } from "../db/entities.js";
import { unixSecondsNow } from "../time.js";

const BATCH_SIZE = Math.min(
  500,
  Math.max(1, Number(process.env.OFFER_DISPATCH_BATCH_SIZE ?? 50) || 50),
);
const TICK_MS = Math.min(
  60_000,
  Math.max(50, Number(process.env.OFFER_DISPATCH_INTERVAL_MS ?? 250) || 250),
);

/**
 * Polls `offer_orders` with `sequence_id` greater than `last_sent_sequence_id`,
 * sends JSON with `input_sequence_id` = `sequence_id`, then advances
 * `last_sent_sequence_id` to the batch maximum.
 */
export class OfferDispatchService {
  private timer: ReturnType<typeof setInterval> | null = null;
  private inFlight = false;

  constructor(private readonly producer: Producer) {}

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick().catch((e) =>
        console.error("[offer-dispatch] tick failed:", e instanceof Error ? e.message : e),
      );
    }, TICK_MS);
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    while (this.inFlight) {
      await new Promise<void>((r) => setTimeout(r, 20));
    }
  }

  private async tick(): Promise<void> {
    if (this.inFlight) return;
    this.inFlight = true;
    try {
      const mgr = getDb().manager;

      const state = await mgr.findOne(OfferDispatchState, { where: { id: 1 } });
      if (!state) {
        console.error(
          "[offer-dispatch] cannot dispatch: missing offer_dispatch_state row id=1; run database migrations.",
        );
        throw new Error("offer_dispatch_state row missing; run migrations");
      }

      const lastSent = state.lastSentSequenceId;
      const filtered = await mgr
        .createQueryBuilder(OfferOrder, "o")
        .where("o.sequenceId IS NOT NULL")
        .andWhere("o.sequenceId > :last", { last: lastSent })
        .orderBy("o.sequenceId", "ASC")
        .take(BATCH_SIZE)
        .getMany();

      if (filtered.length === 0) return;

      const topic = `offer.${getAppConfig().marketName}`;

      const kafkaMessages: { value: string }[] = [];
      for (const row of filtered) {
        const seq = row.sequenceId!;
        let base: Record<string, unknown>;
        try {
          base = JSON.parse(row.payloadJson) as Record<string, unknown>;
        } catch (e) {
          const detail = e instanceof Error ? e.message : String(e);
          console.error(
            `[offer-dispatch] cannot dispatch: invalid JSON in payload_json for order_id=${row.id} sequence_id=${seq}: ${detail}`,
          );
          return;
        }
        const body = { ...base, input_sequence_id: seq };
        kafkaMessages.push({ value: JSON.stringify(body) });
      }

      try {
        await this.producer.send({
          topic,
          messages: kafkaMessages,
        });
      } catch (e) {
        const detail = e instanceof Error ? e.message : String(e);
        console.error(
          `[offer-dispatch] cannot write to Kafka: topic=${topic} batch_size=${filtered.length}: ${detail}`,
        );
        throw e;
      }

      const lastRow = filtered[filtered.length - 1]!;
      const maxSentSeq = lastRow.sequenceId!;
      const ids = filtered.map((r) => r.id);
      const now = unixSecondsNow();

      try {
        await mgr.transaction(async (em) => {
          await em.update(
            OfferDispatchState,
            { id: 1 },
            {
              lastSentSequenceId: maxSentSeq,
              updatedAt: now,
            },
          );
          await em.update(OfferOrder, { id: In(ids) }, { status: "sent" });
        });
      } catch (e) {
        const detail = e instanceof Error ? e.message : String(e);
        console.error(
          `[offer-dispatch] Kafka send succeeded but DB update failed (possible inconsistency): topic=${topic} max_sequence=${maxSentSeq}: ${detail}`,
        );
        throw e;
      }
    } finally {
      this.inFlight = false;
    }
  }
}
