import { Mutex } from "async-mutex";
import { getAppConfig } from "../config.js";
import { getDb } from "../db.js";
import { OfferOrder } from "../db/entities.js";
import { unixSecondsNow } from "../time.js";
import type { OfferSequenceService } from "./offerSequenceService.js";

/** Matches `extern_id` / Kafka message `id`; MySQL signed INT */
const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

export class OfferValidationError extends Error {}

function validationError(message: string): never {
  throw new OfferValidationError(message);
}

function decimalStringIsPositive(whole: string, frac: string): boolean {
  return /[1-9]/.test(whole) || /[1-9]/.test(frac);
}

function pricePrecision(): number {
  const { stock_prec, money_prec } = getAppConfig().yaml.market;
  return Math.max(0, money_prec - stock_prec);
}

function normalizeDecimalField(
  raw: unknown,
  scale: number,
  field: string,
  context: string,
): string {
  if (raw === undefined || raw === null || String(raw).trim() === "") {
    validationError(`${context} failed: ${field} is required`);
  }

  const value = String(raw).trim();
  const m = value.match(/^(\d+)(?:\.(\d*))?$/);
  if (!m) {
    validationError(`${context} failed: invalid ${field}`);
  }

  const frac = m[2] ?? "";
  if (frac.length > scale) {
    validationError(`${context} failed: ${field} precision exceeds ${scale}`);
  }

  const whole = m[1]!.replace(/^0+(?=\d)/, "") || "0";
  const normalizedFrac = frac.replace(/0+$/, "");
  const normalized =
    normalizedFrac.length > 0 ? `${whole}.${normalizedFrac}` : whole;
  if (!decimalStringIsPositive(whole, normalizedFrac)) {
    validationError(`${context} failed: ${field} must be > 0`);
  }
  return normalized;
}

function parseUserId(body: Record<string, unknown>, context: string): number {
  const userId = Number(body.user_id);
  if (!Number.isInteger(userId) || userId <= 0) {
    validationError(`${context} failed: invalid or missing user_id`);
  }
  return userId;
}

function parseSide(body: Record<string, unknown>, context: string): number {
  const side = Number(body.side);
  if (side !== 1 && side !== 2) {
    validationError(`${context} failed: invalid side (use 1=ask, 2=bid)`);
  }
  return side;
}

function parseExternId(body: Record<string, unknown>): number {
  const raw = body.extern_id;
  if (raw !== undefined && raw !== null && raw !== "") {
    const n = Number(raw);
    if (!Number.isFinite(n)) validationError("invalid extern_id (not a finite number)");
    const t = Math.trunc(n);
    if (t < INT32_MIN || t > INT32_MAX) {
      validationError(`extern_id out of INT32 range [${INT32_MIN}, ${INT32_MAX}]`);
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
      const cfg = getAppConfig().yaml.market;
      const user_id = parseUserId(body, "limit order");
      const side = parseSide(body, "limit order");
      const amount = normalizeDecimalField(body.amount, cfg.stock_prec, "amount", "limit order");
      const price = normalizeDecimalField(body.price, pricePrecision(), "price", "limit order");
      const takerFeeRate = normalizeDecimalField(
        body.taker_fee_rate ?? "0.1",
        cfg.fee_prec,
        "taker_fee_rate",
        "limit order",
      );
      const makerFeeRate = normalizeDecimalField(
        body.maker_fee_rate ?? "0.1",
        cfg.fee_prec,
        "maker_fee_rate",
        "limit order",
      );
      const extern_id = parseExternId(body);
      const summary = `limit user=${user_id} side=${side} price=${price} amount=${amount}`;
      const payload = {
        method: "order.put_limit",
        id: extern_id,
        params: {
          user_id,
          side,
          amount,
          price,
          taker_fee_rate: takerFeeRate,
          maker_fee_rate: makerFeeRate,
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
      const cfg = getAppConfig().yaml.market;
      const user_id = parseUserId(body, "market order");
      const side = parseSide(body, "market order");
      const amountScale = side === 1 ? cfg.stock_prec : cfg.money_prec;
      const amount = normalizeDecimalField(body.amount, amountScale, "amount", "market order");
      const takerFeeRate = normalizeDecimalField(
        body.taker_fee_rate ?? "0.1",
        cfg.fee_prec,
        "taker_fee_rate",
        "market order",
      );
      const extern_id = parseExternId(body);
      const summary = `market user=${user_id} side=${side} amount=${amount}`;
      const payload = {
        method: "order.put_market",
        id: extern_id,
        params: {
          user_id,
          side,
          amount,
          taker_fee_rate: takerFeeRate,
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
      const user_id = parseUserId(body, "cancel");
      const order_id_param = Number(body.order_id);
      if (!Number.isInteger(order_id_param) || order_id_param <= 0) {
        validationError("cancel failed: invalid or missing order_id");
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
