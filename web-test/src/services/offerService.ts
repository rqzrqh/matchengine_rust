import { Mutex } from "async-mutex";
import { desc, max } from "drizzle-orm";
import { getAppConfig } from "../config.js";
import { getDb } from "../db.js";
import { unixSecondsNow } from "../time.js";
import { offerOrders, type OfferOrder } from "../db/schema.js";

/** Matches `extern_id` / Kafka message `id`; MySQL signed INT */
const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

function parseExternId(body: Record<string, unknown>): number {
  const raw = body.extern_id;
  if (raw !== undefined && raw !== null && raw !== "") {
    const n = Number(raw);
    if (!Number.isFinite(n)) throw new Error("invalid extern_id");
    const t = Math.trunc(n);
    if (t < INT32_MIN || t > INT32_MAX) {
      throw new Error(`extern_id out of INT range [${INT32_MIN}, ${INT32_MAX}]`);
    }
    return t;
  }
  return Math.floor(Math.random() * INT32_MAX) + 1;
}

export type OfferKind = "limit" | "market" | "cancel";

export type OfferOrderApiRow = {
  id: number;
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
 * Offer service: `offer_orders.id` matches `input_sequence_id` in the payload;
 * Kafka send is done asynchronously by {@link OfferDispatchService}.
 */
export class OfferService {
  private readonly seqMutex = new Mutex();

  async listOrders(): Promise<OfferOrderApiRow[]> {
    const db = getDb();
    const rows = await db
      .select()
      .from(offerOrders)
      .orderBy(desc(offerOrders.id))
      .limit(500);
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
      const db = getDb();
      const [agg] = await db.select({ m: max(offerOrders.id) }).from(offerOrders);
      const id = (agg.m ?? 0) + 1;
      const body = { ...payload, input_sequence_id: id };
      const payloadJson = JSON.stringify(body);
      const createdAt = unixSecondsNow();

      await db.insert(offerOrders).values({
        id,
        createdAt,
        kind,
        externId,
        userId,
        summary,
        status: "pending",
        error: null,
        payloadJson,
      });

      const offerTopic = `offer.${getAppConfig().marketName}`;
      return { ok: true, input_sequence_id: id, topic: offerTopic, order_id: id };
    });
  }

  async placeLimit(body: Record<string, unknown>): Promise<{
    ok: boolean;
    input_sequence_id: number;
    topic: string;
    order_id: number;
  }> {
    const user_id = Number(body.user_id);
    const side = Number(body.side);
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
    return this.sendOffer("limit", user_id, extern_id, summary, payload);
  }

  async placeMarket(body: Record<string, unknown>): Promise<{
    ok: boolean;
    input_sequence_id: number;
    topic: string;
    order_id: number;
  }> {
    const user_id = Number(body.user_id);
    const side = Number(body.side);
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
    return this.sendOffer("market", user_id, extern_id, summary, payload);
  }

  async cancel(body: Record<string, unknown>): Promise<{
    ok: boolean;
    input_sequence_id: number;
    topic: string;
    order_id: number;
  }> {
    const user_id = Number(body.user_id);
    const order_id_param = Number(body.order_id);
    const extern_id = parseExternId(body);
    const summary = `cancel user=${user_id} order_id=${order_id_param}`;
    const payload = {
      method: "order.cancel",
      id: extern_id,
      params: { user_id, order_id: order_id_param },
    };
    return this.sendOffer("cancel", user_id, extern_id, summary, payload);
  }
}
