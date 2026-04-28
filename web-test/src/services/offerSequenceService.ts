import { getDb } from "../db.js";
import { OfferDispatchState, OfferOrder } from "../db/entities.js";

const BATCH_SIZE = Math.min(
  500,
  Math.max(1, Number(process.env.OFFER_SEQUENCE_BATCH_SIZE ?? 200) || 200),
);
const TICK_MS = Math.min(
  60_000,
  Math.max(25, Number(process.env.OFFER_SEQUENCE_INTERVAL_MS ?? 100) || 100),
);

/**
 * Assigns monotonic `sequence_id` to rows where it is still null, ordered by `offer_orders.id`.
 * Uses only `offer_dispatch_state.last_sequence_id`: next assigned id is `last_sequence_id + 1`, then +2, …
 */
export class OfferSequenceService {
  private timer: ReturnType<typeof setInterval> | null = null;
  private inFlight = false;

  async assignPending(): Promise<void> {
    const ds = getDb();
    await ds.manager.transaction(async (em) => {
      const st = await em
        .createQueryBuilder(OfferDispatchState, "s")
        .setLock("pessimistic_write")
        .where("s.id = :id", { id: 1 })
        .getOne();
      const base = st?.lastSequenceId ?? 0;
      let next = base + 1;

      const pending = await em
        .createQueryBuilder(OfferOrder, "o")
        .setLock("pessimistic_write")
        .where("o.sequenceId IS NULL")
        .orderBy("o.id", "ASC")
        .take(BATCH_SIZE)
        .getMany();

      for (const row of pending) {
        await em.update(OfferOrder, { id: row.id }, { sequenceId: next });
        next += 1;
      }
    });
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      if (this.inFlight) return;
      this.inFlight = true;
      void this.assignPending()
        .catch((e) => console.error("[offer-sequence]", e))
        .finally(() => {
          this.inFlight = false;
        });
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
}
