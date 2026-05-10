import { getDb } from "../db.js";
import { OfferDispatchState, OfferOrder } from "../db/entities.js";
import { unixSecondsNow } from "../time.js";

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
 * Advances `offer_dispatch_state.last_sequence_id` with a compare-and-set guard in the same
 * transaction, so another sequencer cannot race and reuse an already assigned sequence id.
 */
export class OfferSequenceService {
  private timer: ReturnType<typeof setInterval> | null = null;
  private inFlight = false;

  async assignPending(): Promise<boolean> {
    const ds = getDb();
    return ds.manager.transaction(async (em) => {
      const st = await em.findOne(OfferDispatchState, { where: { id: 1 } });
      if (!st) {
        throw new Error("offer_dispatch_state row missing; run database migrations");
      }

      const pending = await em
        .createQueryBuilder(OfferOrder, "o")
        .setLock("pessimistic_write")
        .where("o.sequenceId IS NULL")
        .orderBy("o.id", "ASC")
        .take(BATCH_SIZE)
        .getMany();

      if (pending.length === 0) return false;

      const base = st.lastSequenceId;
      let next = base + 1;
      for (const row of pending) {
        await em.update(OfferOrder, { id: row.id }, { sequenceId: next });
        next += 1;
      }

      const updateResult = await em
        .createQueryBuilder()
        .update(OfferDispatchState)
        .set({ lastSequenceId: next - 1, updatedAt: unixSecondsNow() })
        .where("id = :id", { id: 1 })
        .andWhere("last_sequence_id = :base", { base })
        .execute();

      if ((updateResult.affected ?? 0) !== 1) {
        throw new Error(
          `offer sequence CAS failed: expected last_sequence_id=${base}, assigned=${pending.length}`,
        );
      }

      return pending.length === BATCH_SIZE;
    });
  }

  start(): void {
    if (this.timer) return;
    void this.tick().catch((e) => console.error("[offer-sequence]", e));
    this.timer = setInterval(() => {
      void this.tick().catch((e) => console.error("[offer-sequence]", e));
    }, TICK_MS);
  }

  private async tick(): Promise<void> {
    if (this.inFlight) return;
    this.inFlight = true;
    try {
      while (await this.assignPending()) {
        // Drain current pending backlog without waiting for the next timer tick.
      }
    } finally {
      this.inFlight = false;
    }
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
