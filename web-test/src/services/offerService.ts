import { Mutex } from "async-mutex";
import { getAppConfig } from "../config.js";
import { getDb } from "../db.js";
import { OfferOrder } from "../db/entities.js";
import { unixSecondsNow } from "../time.js";
import type { OfferSequenceService } from "./offerSequenceService.js";

/** Matches `extern_id` / Kafka message `id`; MySQL signed INT */
const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

function parseExternId(body: Record<string, unknown>): number {
  const raw = body.extern_id;
  if (raw !== undefined && raw !== null && raw !== "") {
    const n = Number(raw);
    if (!Number.isFinite(n)) throw new Error("invalid extern_id (not a finite number)");
    const t = Math.trunc(n);
    if (t < INT32_MIN || t > INT32_MAX) {
      throw new Error(`extern_id out of INT32 range [${INT32_MIN}, ${INT32_MAX}]`);
    }
    return t;
  }
  return Math.floor(Math.random() * INT32_MAX) + 1;
}

export type OfferKind = "limit" | "market" | "cancel";

export type OfferOrderApiRow = {
  id: number;
  sequence_id: number | null;
  created_at: number;
  kind: OfferKind;
  extern_id: number;
  user_id: number;
  summary: string;
  status: "pending" | "sent" | "failed";
  error?: string;
};

function toApiRow(r: OfferOrder): OfferOrderApiRow {
  return {
    id: r.id,
    sequence_id: r.sequenceId ?? null,
    created_at: r.createdAt,
    kind: r.kind as OfferKind,
    extern_id: r.externId,
    user_id: r.userId,
    summary: r.summary,
    status: r.status as OfferOrderApiRow["status"],
    error: r.error ?? undefined,
  };
}

/**
 * Offer service: table `id` is unique row id; `input_sequence_id` in Kafka comes from `sequence_id`
 * (assigned by {@link OfferSequenceService}). Kafka send is done by {@link OfferDispatchService}.
 */
export class OfferService {
  private readonly seqMutex = new Mutex();

  constructor(private readonly sequence: OfferSequenceService) {}

  async listOrders(): Promise<OfferOrderApiRow[]> {
    const mgr = getDb().manager;
    const rows = await mgr.find(OfferOrder, {
      order: { id: "DESC" },
      take: 500,
    });
    return rows.map(toApiRow);
  }

  private async sendOffer(
    kind: OfferKind,
    userId: number,
    externId: number,
    summary: string,
    payload: Record<string, unknown>,
  ): Promise<{ ok: boolean; input_sequence_id: number; topic: string; order_id: number }> {
    return this.seqMutex.runExclusive(async () => {
      const mgr = getDb().manager;
      const raw = await mgr
        .createQueryBuilder(OfferOrder, "o")
        .select("MAX(o.id)", "max")
        .getRawOne<{ max: string | number | null }>();
      const maxId = raw?.max != null ? Number(raw.max) : 0;
      const id = maxId + 1;
      const payloadJson = JSON.stringify(payload);
      const createdAt = unixSecondsNow();

      await mgr.insert(OfferOrder, {
        id,
        sequenceId: null,
        createdAt,
        kind,
        externId,
        userId,
        summary,
        status: "pending",
        error: null,
        payloadJson,
      });

      await this.sequence.assignPending();

      const row = await mgr.findOne(OfferOrder, { where: { id } });
      if (row?.sequenceId == null) {
        throw new Error(
          "order row inserted but sequence_id was not assigned (sequencing failed); check DB and offer_sequence worker",
        );
      }
      const seq = row.sequenceId;

      const offerTopic = `offer.${getAppConfig().marketName}`;
      return { ok: true, input_sequence_id: seq, topic: offerTopic, order_id: id };
    });
  }

  private logOfferFailure(kindLabel: string, e: unknown): void {
    const msg = e instanceof Error ? e.message : String(e);
    console.warn(`[offer] cannot place order (${kindLabel}): ${msg}`);
  }

  async placeLimit(body: Record<string, unknown>): Promise<{
    ok: boolean;
    input_sequence_id: number;
    topic: string;
    order_id: number;
  }> {
    try {
      const user_id = Number(body.user_id);
      if (!Number.isFinite(user_id)) {
        throw new Error("limit order failed: invalid or missing user_id");
      }
      const side = Number(body.side);
      if (!Number.isFinite(side)) {
        throw new Error("limit order failed: invalid or missing side");
      }
      if (body.amount === undefined || body.amount === null || String(body.amount).trim() === "") {
        throw new Error("limit order failed: amount is required");
      }
      if (body.price === undefined || body.price === null || String(body.price).trim() === "") {
        throw new Error("limit order failed: price is required");
      }
      const extern_id = parseExternId(body);
      const summary = `limit user=${user_id} side=${side} price=${body.price} amount=${body.amount}`;
      const payload = {
        method: "order.put_limit",
        id: extern_id,
        params: {
          user_id,
          side,
          amount: String(body.amount),
          price: String(body.price),
          taker_fee_rate: String(body.taker_fee_rate ?? "0.1"),
          maker_fee_rate: String(body.maker_fee_rate ?? "0.1"),
        },
      };
      return await this.sendOffer("limit", user_id, extern_id, summary, payload);
    } catch (e) {
      this.logOfferFailure("limit", e);
      throw e;
    }
  }

  async placeMarket(body: Record<string, unknown>): Promise<{
    ok: boolean;
    input_sequence_id: number;
    topic: string;
    order_id: number;
  }> {
    try {
      const user_id = Number(body.user_id);
      if (!Number.isFinite(user_id)) {
        throw new Error("market order failed: invalid or missing user_id");
      }
      const side = Number(body.side);
      if (!Number.isFinite(side)) {
        throw new Error("market order failed: invalid or missing side");
      }
      if (body.amount === undefined || body.amount === null || String(body.amount).trim() === "") {
        throw new Error("market order failed: amount is required");
      }
      const extern_id = parseExternId(body);
      const summary = `market user=${user_id} side=${side} amount=${body.amount}`;
      const payload = {
        method: "order.put_market",
        id: extern_id,
        params: {
          user_id,
          side,
          amount: String(body.amount),
          taker_fee_rate: String(body.taker_fee_rate ?? "0.1"),
        },
      };
      return await this.sendOffer("market", user_id, extern_id, summary, payload);
    } catch (e) {
      this.logOfferFailure("market", e);
      throw e;
    }
  }

  async cancel(body: Record<string, unknown>): Promise<{
    ok: boolean;
    input_sequence_id: number;
    topic: string;
    order_id: number;
  }> {
    try {
      const user_id = Number(body.user_id);
      if (!Number.isFinite(user_id)) {
        throw new Error("cancel failed: invalid or missing user_id");
      }
      const order_id_param = Number(body.order_id);
      if (!Number.isFinite(order_id_param)) {
        throw new Error("cancel failed: invalid or missing order_id");
      }
      const extern_id = parseExternId(body);
      const summary = `cancel user=${user_id} order_id=${order_id_param}`;
      const payload = {
        method: "order.cancel",
        id: extern_id,
        params: { user_id, order_id: order_id_param },
      };
      return await this.sendOffer("cancel", user_id, extern_id, summary, payload);
    } catch (e) {
      this.logOfferFailure("cancel", e);
      throw e;
    }
  }
}
