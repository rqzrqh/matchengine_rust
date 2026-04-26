import { asc, eq, gt, inArray } from "drizzle-orm";
import type { Producer } from "kafkajs";
import { getAppConfig } from "../config.js";
import { getDb } from "../db.js";
import { offerDispatchState, offerOrders } from "../db/schema.js";
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
 * Polls `offer_orders` for rows with `id` greater than `offer_dispatch_state.last_sent_order_id`,
 * sends a batch to Kafka, then updates progress and row status in one transaction.
 */
export class OfferDispatchService {
  private timer: ReturnType<typeof setInterval> | null = null;
  private inFlight = false;

  constructor(private readonly producer: Producer) {}

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick().catch((e) => console.error("[offer-dispatch]", e));
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
      const db = getDb();

      const [state] = await db
        .select()
        .from(offerDispatchState)
        .where(eq(offerDispatchState.id, 1))
        .limit(1);
      if (!state) {
        throw new Error("offer_dispatch_state row missing; run migrations");
      }

      const lastSent = state.lastSentOrderId;
      const pending = await db
        .select()
        .from(offerOrders)
        .where(gt(offerOrders.id, lastSent))
        .orderBy(asc(offerOrders.id))
        .limit(BATCH_SIZE);

      if (pending.length === 0) return;

      const topic = `offer.${getAppConfig().marketName}`;
      await this.producer.send({
        topic,
        messages: pending.map((row) => ({ value: row.payloadJson })),
      });

      const maxId = pending[pending.length - 1]!.id;
      const ids = pending.map((r) => r.id);
      const now = unixSecondsNow();

      await db.transaction(async (tx) => {
        await tx
          .update(offerDispatchState)
          .set({ lastSentOrderId: maxId, updatedAt: now })
          .where(eq(offerDispatchState.id, 1));
        await tx
          .update(offerOrders)
          .set({ status: "sent" })
          .where(inArray(offerOrders.id, ids));
      });
    } finally {
      this.inFlight = false;
    }
  }
}
