import "./style.css";

interface ApiConfig {
  market_name: string;
  engine_url?: string;
}

interface ApiErrorBody {
  detail?: string;
}

function buildHttpErrorMessage(r: Response, data: unknown): string {
  const body = data as ApiErrorBody & { error?: string };
  if (typeof body?.detail === "string" && body.detail.trim() !== "") {
    return body.detail;
  }
  if (typeof body?.error === "string" && body.error.trim() !== "") {
    return body.error;
  }
  if (typeof data === "string" && data.trim() !== "") {
    return data;
  }
  if (data !== null && data !== undefined && typeof data === "object") {
    const s = JSON.stringify(data);
    if (s !== "{}" && s !== "null") return s;
  }
  return `Request failed (HTTP ${r.status}${r.statusText ? ` ${r.statusText}` : ""})`;
}

/** Same default as web-test; `loadConfig` overwrites with `engine_url` from `GET /api/config`. */
const DEFAULT_ENGINE_HTTP = "http://127.0.0.1:8080";

/** Minimum/initial interval used when HTTP auto refresh is enabled. */
const MIN_HTTP_REFRESH_MS = 500;
const DEFAULT_HTTP_REFRESH_MS = 1000;
const AUTO_ORDER_BOOK_DEPTH_LIMIT = 500;
const AUTO_SIDE_IMBALANCE_THRESHOLD = 0.15;
const AUTO_CORRECTION_INTERVAL_MIN_MS = 3_000;
const AUTO_CORRECTION_INTERVAL_MAX_MS = 5_000;
const AUTO_CORRECTION_RATIO = 0.05;
const AUTO_MAX_CONCURRENCY = 16;
const MARKET_BUY_AMOUNT_BUFFER = 1.000001;

let marketName = __MATCENGINE_CONFIG__.market.name;
let engineHttpBase = DEFAULT_ENGINE_HTTP;
let previousEngineStatus: EngineStatusNumericSnapshot | null = null;
let lastHttpRefreshText = "Waiting for first refresh";

function nonNegativeInt(v: unknown, fallback = 0): number {
  const n = Number(v);
  return Number.isInteger(n) && n >= 0 ? n : fallback;
}

const marketPrecision = {
  stock: nonNegativeInt(__MATCENGINE_CONFIG__.market.stock_prec),
  money: nonNegativeInt(__MATCENGINE_CONFIG__.market.money_prec),
  fee: nonNegativeInt(__MATCENGINE_CONFIG__.market.fee_prec),
};

const pricePrecision = Math.max(0, marketPrecision.money - marketPrecision.stock);

async function loadConfig(): Promise<void> {
  try {
    const r = await fetch("/api/config");
    if (!r.ok) return;
    const c = (await r.json()) as ApiConfig;
    if (c.market_name) marketName = c.market_name;
    if (typeof c.engine_url === "string" && c.engine_url.length > 0) {
      engineHttpBase = c.engine_url.replace(/\/$/, "");
    }
  } catch {
    /* keep defaults */
  }
}

/** Browser calls the matching engine REST API directly (engine must allow this origin in CORS). */
async function engineApi<T>(path: string): Promise<T> {
  const p = path.startsWith("/") ? path : `/${path}`;
  const r = await fetch(`${engineHttpBase}${p}`);
  const text = await r.text();
  let data: unknown;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  if (!r.ok) {
    throw new Error(buildHttpErrorMessage(r, data));
  }
  return data as T;
}

type EngineStatusPayload = {
  oper_id?: number | string | null;
  order_id?: number | string | null;
  deals_id?: number | string | null;
  message_id?: number | string | null;
  settle_message_ids?: Array<number | string | null> | null;
  input_offset?: number | string | null;
  input_sequence_id?: number | string | null;
  pushed_quote_deals_id?: number | string | null;
  pushed_settle_message_ids?: Array<number | string | null> | null;
  error?: string | null;
};

type EngineStatusNumericSnapshot = {
  oper_id: bigint | null;
  order_id: bigint | null;
  deals_id: bigint | null;
  message_id: bigint | null;
  input_offset: bigint | null;
  input_sequence_id: bigint | null;
};

async function api<T>(path: string, opts: RequestInit = {}): Promise<T> {
  const hasJsonBody = opts.body !== undefined && typeof opts.body === "string";
  const r = await fetch(path, {
    ...opts,
    headers: {
      ...(hasJsonBody ? { "Content-Type": "application/json" } : {}),
      ...(opts.headers as Record<string, string> | undefined),
    },
  });
  const text = await r.text();
  const ct = r.headers.get("content-type") ?? "";
  if (
    r.ok &&
    path.startsWith("/api") &&
    ct.includes("text/html")
  ) {
    throw new Error(
      "API returned HTML instead of JSON: open the app on web-test :8000 (npm run dev), or run the backend in another terminal and keep Vite’s /api proxy when using Vite alone.",
    );
  }
  let data: unknown;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  if (!r.ok) {
    throw new Error(buildHttpErrorMessage(r, data));
  }
  return data as T;
}

function show(el: HTMLElement, obj: unknown): void {
  el.textContent = typeof obj === "string" ? obj : JSON.stringify(obj, null, 2);
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** Common engine order JSON field order, and display labels for row hover `title` tooltips. */
const ORDER_DETAIL_KEYS: readonly string[] = [
  "id",
  "type",
  "side",
  "create_time",
  "update_time",
  "user_id",
  "price",
  "amount",
  "taker_fee_rate",
  "maker_fee_rate",
  "left",
  "deal_stock",
  "deal_money",
  "deal_fee",
];
const ORDER_DETAIL_LABEL: Record<string, string> = {
  id: "order id",
  type: "type",
  side: "side",
  create_time: "create_time",
  update_time: "update_time",
  user_id: "user_id",
  price: "price",
  amount: "amount",
  taker_fee_rate: "taker_fee_rate",
  maker_fee_rate: "maker_fee_rate",
  left: "left",
  deal_stock: "deal_stock",
  deal_money: "deal_money",
  deal_fee: "deal_fee",
};

function formatOrderDetailTitle(o: Record<string, unknown>): string {
  const lines: string[] = [];
  const seen = new Set<string>();
  for (const k of ORDER_DETAIL_KEYS) {
    if (Object.prototype.hasOwnProperty.call(o, k)) {
      seen.add(k);
      lines.push(formatOrderFieldLine(k, o[k]));
    }
  }
  for (const k of Object.keys(o).sort()) {
    if (seen.has(k)) continue;
    lines.push(formatOrderFieldLine(k, o[k]));
  }
  return lines.join("\n");
}

function formatOrderFieldLine(k: string, v: unknown): string {
  const label = ORDER_DETAIL_LABEL[k] ?? k;
  if (k === "side") {
    const n = typeof v === "number" ? v : Number(v);
    const hint =
      n === 1 ? " (sell / ask)" : n === 2 ? " (buy / bid)" : "";
    return `${label}: ${v === null || v === undefined ? "—" : String(v)}${hint}`;
  }
  return `${label}: ${v === null || v === undefined ? "—" : String(v)}`;
}

function renderOrderRows(
  tbody: HTMLTableSectionElement,
  data: unknown,
  errorText?: string,
  fillFromBottom = false,
): void {
  tbody.replaceChildren();
  if (errorText !== undefined) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 5;
    td.className = "ob-table-err";
    td.textContent = errorText;
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  const rec = data as { orders?: Array<Record<string, unknown>> };
  const orders = rec.orders;
  if (!Array.isArray(orders) || orders.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 5;
    td.className = "ob-table-empty";
    td.textContent = "No orders";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  const toShow = fillFromBottom ? [...orders].reverse() : orders;
  for (const o of toShow) {
    const tr = document.createElement("tr");
    tr.className = "ob-order-row";
    tr.title = formatOrderDetailTitle(o);
    tr.innerHTML = [
      escapeHtml(String(o.id)),
      escapeHtml(String(o.user_id)),
      escapeHtml(String(o.price)),
      escapeHtml(String(o.left)),
      escapeHtml(String(o.amount)),
    ]
      .map((c) => `<td>${c}</td>`)
      .join("");
    tbody.appendChild(tr);
  }
}

function formJson(form: HTMLFormElement): Record<string, string> {
  const fd = new FormData(form);
  const o: Record<string, string> = {};
  fd.forEach((v, k) => {
    o[k] = String(v);
  });
  return o;
}

function $(id: string): HTMLElement {
  const el = document.getElementById(id);
  if (!el) throw new Error(`missing #${id}`);
  return el;
}

async function refreshEngineStatus(): Promise<void> {
  const operEl = document.getElementById("st-oper-id");
  const orderEl = document.getElementById("st-order-id");
  const dealsEl = document.getElementById("st-deals-id");
  const messageEl = document.getElementById("st-message-id");
  const offsetEl = document.getElementById("st-input-offset");
  const seqEl = document.getElementById("st-input-seq-id");
  const operDeltaEl = document.getElementById("st-oper-id-delta");
  const orderDeltaEl = document.getElementById("st-order-id-delta");
  const dealsDeltaEl = document.getElementById("st-deals-id-delta");
  const messageDeltaEl = document.getElementById("st-message-id-delta");
  const offsetDeltaEl = document.getElementById("st-input-offset-delta");
  const seqDeltaEl = document.getElementById("st-input-seq-id-delta");
  const errEl = document.getElementById("st-engine-err");
  if (
    !operEl ||
    !orderEl ||
    !dealsEl ||
    !messageEl ||
    !offsetEl ||
    !seqEl ||
    !operDeltaEl ||
    !orderDeltaEl ||
    !dealsDeltaEl ||
    !messageDeltaEl ||
    !offsetDeltaEl ||
    !seqDeltaEl
  ) {
    return;
  }
  try {
    const marketStatus = await engineApi<EngineStatusPayload>(
      `/markets/${encodeURIComponent(marketName)}/status`,
    );
    const currentSnapshot: EngineStatusNumericSnapshot = {
      oper_id: parseBigIntOrNull(marketStatus.oper_id),
      order_id: parseBigIntOrNull(marketStatus.order_id),
      deals_id: parseBigIntOrNull(marketStatus.deals_id),
      message_id: parseBigIntOrNull(marketStatus.message_id),
      input_offset: parseBigIntOrNull(marketStatus.input_offset),
      input_sequence_id: parseBigIntOrNull(marketStatus.input_sequence_id),
    };
    setText(operEl, marketStatus.oper_id);
    setText(orderEl, marketStatus.order_id);
    setText(dealsEl, marketStatus.deals_id);
    setText(messageEl, marketStatus.message_id);
    setText(offsetEl, marketStatus.input_offset);
    setText(seqEl, marketStatus.input_sequence_id);
    setStatusDelta(operDeltaEl, currentSnapshot.oper_id, previousEngineStatus?.oper_id ?? null);
    setStatusDelta(orderDeltaEl, currentSnapshot.order_id, previousEngineStatus?.order_id ?? null);
    setStatusDelta(dealsDeltaEl, currentSnapshot.deals_id, previousEngineStatus?.deals_id ?? null);
    setStatusDelta(messageDeltaEl, currentSnapshot.message_id, previousEngineStatus?.message_id ?? null);
    setStatusDelta(offsetDeltaEl, currentSnapshot.input_offset, previousEngineStatus?.input_offset ?? null);
    setStatusDelta(
      seqDeltaEl,
      currentSnapshot.input_sequence_id,
      previousEngineStatus?.input_sequence_id ?? null,
    );
    previousEngineStatus = currentSnapshot;
    setErrLine(errEl, null);
  } catch (e) {
    setText(operEl, "—");
    setText(orderEl, "—");
    setText(dealsEl, "—");
    setText(messageEl, "—");
    setText(offsetEl, "—");
    setText(seqEl, "—");
    setStatusDelta(operDeltaEl, null, null);
    setStatusDelta(orderDeltaEl, null, null);
    setStatusDelta(dealsDeltaEl, null, null);
    setStatusDelta(messageDeltaEl, null, null);
    setStatusDelta(offsetDeltaEl, null, null);
    setStatusDelta(seqDeltaEl, null, null);
    setErrLine(errEl, e instanceof Error ? e.message : String(e));
  }
}

type InputProgressPayload = {
  topic?: string | null;
  kafka_high?: string | null;
  input_offset?: string | null;
  lag?: string | null;
  error?: string | null;
};

type UserOrdersPayload = {
  orders?: Array<Record<string, unknown>>;
};

type MarketSummaryPayload = {
  ask_stock_amount?: number | string | null;
  bid_stock_amount?: number | string | null;
};

type OrderBookPayload = {
  total?: number | string | null;
  orders?: Array<Record<string, unknown>>;
};

type AutoActionKind = "limit" | "market" | "cancel";
type ManualTabKind = "limit" | "market" | "cancel";

type AutoOrderConfig = {
  userStart: number;
  userEnd: number;
  intervalMs: number;
  concurrency: number;
  priceMin: string;
  priceMax: string;
  amountMin: string;
  amountMax: string;
  restingDepth: string;
  actions: AutoActionKind[];
};

type AutoOrderWorkerState = {
  workerIndex: number;
  timerId: number | undefined;
  correctionTimerId: number | undefined;
  nextUserId: number | null;
};

const autoOrderState = {
  running: false,
  workerCount: 0,
  workers: [] as AutoOrderWorkerState[],
};
let activeManualTab: ManualTabKind = "limit";
const httpRefreshState = {
  enabled: false,
  timerId: 0 as number | undefined,
  inFlight: false,
  queued: false,
};

function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  const s = String(v).trim();
  if (s === "" || s === "null" || s === "undefined") return "—";
  return s;
}

/** Avoid assigning null/undefined to `textContent`—some engines render the literals "null" / "undefined". */
function setText(el: HTMLElement, v: unknown, empty = "—"): void {
  if (v === null || v === undefined) {
    el.textContent = empty;
    return;
  }
  if (typeof v === "string" && (v === "null" || v === "undefined")) {
    el.textContent = empty;
    return;
  }
  const t = String(v).trim();
  if (t === "" || t === "null" || t === "undefined") {
    el.textContent = empty;
    return;
  }
  el.textContent = t;
}

function setErrLine(el: HTMLElement | null, message: string | null | undefined): void {
  if (!el) return;
  if (message == null) {
    el.textContent = "";
    el.hidden = true;
    return;
  }
  const t = String(message).trim();
  if (t === "" || t === "null" || t === "undefined") {
    el.textContent = "";
    el.hidden = true;
    return;
  }
  el.textContent = t;
  el.hidden = false;
}

function parseBigIntOrNull(v: unknown): bigint | null {
  const s = fmtCell(v);
  if (s === "—") return null;
  try {
    return BigInt(s);
  } catch {
    return null;
  }
}

function parseNumberOrNull(v: unknown): number | null {
  const s = fmtCell(v);
  if (s === "—") return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function formatDecimalAtScale(value: number, scale: number, fallback = "0"): string {
  if (!Number.isFinite(value) || value <= 0) return fallback;
  const factor = 10 ** scale;
  const floored = Math.floor(value * factor) / factor;
  if (!Number.isFinite(floored) || floored <= 0) return fallback;
  return floored.toFixed(scale).replace(/\.?0+$/, "");
}

function normalizeDecimalString(raw: string, scale: number): string {
  const trimmed = raw.trim();
  const m = trimmed.match(/^(\d+)(?:\.(\d*))?$/);
  if (!m) return trimmed;

  const whole = m[1]!.replace(/^0+(?=\d)/, "") || "0";
  const frac = (m[2] ?? "").slice(0, scale);
  return scale > 0 && frac.length > 0 ? `${whole}.${frac}` : whole;
}

function normalizePriceInput(raw: string): string {
  return normalizeDecimalString(raw, pricePrecision);
}

function normalizeStockAmountInput(raw: string): string {
  return normalizeDecimalString(raw, marketPrecision.stock);
}

function normalizeMarketAmountInput(side: number, raw: string): string {
  return normalizeDecimalString(raw, side === 2 ? marketPrecision.money : marketPrecision.stock);
}

function formatPrice(value: number, fallback = "0"): string {
  return formatDecimalAtScale(value, pricePrecision, fallback);
}

function formatStockAmount(value: number, fallback = "0"): string {
  return formatDecimalAtScale(value, marketPrecision.stock, fallback);
}

function formatMoneyAmount(value: number, fallback = "0"): string {
  return formatDecimalAtScale(value, marketPrecision.money, fallback);
}

function parsePositiveAmount(v: string): number {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function validatePositiveDecimalInput(raw: string, scale: number, field: string): string {
  const trimmed = raw.trim();
  const m = trimmed.match(/^(\d+)(?:\.(\d*))?$/);
  if (!m) throw new Error(`${field} must be a positive number`);
  if ((m[2] ?? "").length > scale) {
    throw new Error(`${field} precision exceeds ${scale}`);
  }
  const normalized = normalizeDecimalString(trimmed, scale);
  if (parsePositiveAmount(normalized) <= 0) {
    throw new Error(`${field} must be > 0`);
  }
  return normalized;
}

function priceTick(): number {
  return 1 / 10 ** pricePrecision;
}

function autoAmountBounds(cfg: AutoOrderConfig): { minAmount: number; maxAmount: number } {
  const a = parsePositiveAmount(cfg.amountMin);
  const b = parsePositiveAmount(cfg.amountMax);
  const minAmount = Math.min(a || b || 0, b || a || 0);
  const maxAmount = Math.max(a, b);
  return { minAmount, maxAmount };
}

function autoRestingDepthTarget(cfg: AutoOrderConfig): number {
  const { minAmount } = autoAmountBounds(cfg);
  return Math.max(parsePositiveAmount(cfg.restingDepth), minAmount, 1);
}

function bestBookPrice(orders: Array<Record<string, unknown>>, side: number): number | null {
  const prices = orders
    .map((order) => orderPrice(order))
    .filter((price) => Number.isFinite(price) && price > 0);
  if (prices.length === 0) return null;
  return side === 1 ? Math.min(...prices) : Math.max(...prices);
}

function pickRestingLimitPrice(
  side: number,
  asks: Array<Record<string, unknown>>,
  bids: Array<Record<string, unknown>>,
  cfg: AutoOrderConfig,
): string | null {
  const tick = priceTick();
  const cfgMin = parsePositiveAmount(cfg.priceMin);
  const cfgMax = parsePositiveAmount(cfg.priceMax);
  const fallbackMin = cfgMin > 0 ? cfgMin : tick;
  const fallbackMax = cfgMax > 0 ? cfgMax : fallbackMin;
  const bestAsk = bestBookPrice(asks, 1);
  const bestBid = bestBookPrice(bids, 2);

  if (side === 1) {
    let lo = Math.max(fallbackMin, bestBid !== null ? bestBid + tick : fallbackMin);
    let hi = fallbackMax;
    if (bestAsk !== null && bestAsk >= lo) hi = Math.min(hi, bestAsk);
    if (hi < lo) hi = lo;
    return formatPrice(lo === hi ? lo : lo + Math.random() * (hi - lo));
  }

  let hi = Math.min(fallbackMax, bestAsk !== null ? Math.max(bestAsk - tick, tick) : fallbackMax);
  if (hi <= 0) return null;
  let lo = Math.max(Math.min(fallbackMin, hi), bestBid ?? Math.min(fallbackMin, hi));
  if (lo > hi) lo = hi;
  return formatPrice(lo === hi ? lo : lo + Math.random() * (hi - lo));
}

function replenishAmount(cfg: AutoOrderConfig, missingStock: number): string | null {
  const { minAmount, maxAmount } = autoAmountBounds(cfg);
  const fallback = maxAmount > 0 ? maxAmount : minAmount;
  const target = Math.max(minAmount || fallback || 0, missingStock);
  const capped = maxAmount > 0 ? Math.min(target, maxAmount) : target;
  return capped > 0 ? formatStockAmount(capped) : null;
}

function parseBigIntArray(v: unknown): bigint[] {
  if (!Array.isArray(v)) return [];
  return v.map((item) => parseBigIntOrNull(item) ?? 0n);
}

function formatProgress(pushed: bigint | null, current: bigint | null): string {
  if (pushed === null && current === null) return "—";
  return `${pushed ?? 0n}/${current ?? 0n}`;
}

function positiveDiff(current: bigint | null, pushed: bigint | null): bigint {
  if (current === null || pushed === null || current <= pushed) return 0n;
  return current - pushed;
}

function sumIds(ids: bigint[]): bigint {
  return ids.reduce((acc, id) => acc + id, 0n);
}

function setStatusDelta(el: HTMLElement | null, current: bigint | null, previous: bigint | null): void {
  if (!el) return;
  if (current === null || previous === null) {
    el.textContent = "—";
    return;
  }
  const delta = current - previous;
  el.textContent = delta >= 0n ? `+${delta}` : `${delta}`;
}

function setAutoOrderStatus(message: string): void {
  void message;
}

function updateManualSubmitButton(): void {
  const btn = document.getElementById("btn-manual-submit") as HTMLButtonElement | null;
  if (!btn) return;
  btn.textContent =
    activeManualTab === "limit"
      ? "Submit limit order"
      : activeManualTab === "market"
        ? "Submit market order"
        : "Cancel order";
}

function updateAutoOrderButton(): void {
  const btn = document.getElementById("btn-auto-order") as HTMLButtonElement | null;
  if (!btn) return;
  btn.textContent = autoOrderState.running ? "Stop" : "Start";
  setRunningStateButton(btn, autoOrderState.running);
}

function countDecimals(raw: string): number {
  const trimmed = raw.trim();
  const dot = trimmed.indexOf(".");
  return dot >= 0 ? trimmed.length - dot - 1 : 0;
}

function randomDecimalString(minRaw: string, maxRaw: string): string {
  const minNum = Number(minRaw);
  const maxNum = Number(maxRaw);
  if (!Number.isFinite(minNum) || !Number.isFinite(maxNum)) {
    throw new Error("invalid numeric range");
  }
  const lo = Math.min(minNum, maxNum);
  const hi = Math.max(minNum, maxNum);
  const scale = Math.max(countDecimals(minRaw), countDecimals(maxRaw));
  const factor = 10 ** scale;
  const loScaled = Math.round(lo * factor);
  const hiScaled = Math.round(hi * factor);
  const pickScaled =
    loScaled + Math.floor(Math.random() * Math.max(1, hiScaled - loScaled + 1));
  return (pickScaled / factor).toFixed(scale).replace(/\.?0+$/, "");
}

function randomDecimalStringAtScale(minRaw: string, maxRaw: string, scale: number): string {
  return normalizeDecimalString(randomDecimalString(minRaw, maxRaw), scale);
}

function readAutoOrderConfig(): AutoOrderConfig {
  const userStart = Number((document.getElementById("auto-user-start") as HTMLInputElement).value);
  const userEnd = Number((document.getElementById("auto-user-end") as HTMLInputElement).value);
  const intervalMs = Number((document.getElementById("auto-interval-ms") as HTMLInputElement).value);
  const concurrency = Number((document.getElementById("auto-concurrency") as HTMLInputElement).value);
  const priceMin = (document.getElementById("auto-price-min") as HTMLInputElement).value.trim();
  const priceMax = (document.getElementById("auto-price-max") as HTMLInputElement).value.trim();
  const amountMin = (document.getElementById("auto-amount-min") as HTMLInputElement).value.trim();
  const amountMax = (document.getElementById("auto-amount-max") as HTMLInputElement).value.trim();
  const restingDepth = (document.getElementById("auto-resting-depth") as HTMLInputElement).value.trim();

  const actions: AutoActionKind[] = [];
  if ((document.getElementById("auto-enable-limit") as HTMLInputElement).checked) actions.push("limit");
  if ((document.getElementById("auto-enable-market") as HTMLInputElement).checked) actions.push("market");
  if ((document.getElementById("auto-enable-cancel") as HTMLInputElement).checked) actions.push("cancel");

  if (!Number.isInteger(userStart) || !Number.isInteger(userEnd) || userStart <= 0 || userEnd <= 0) {
    throw new Error("user_id range must be positive integers");
  }
  if (userEnd < userStart) {
    throw new Error("user_id end must be >= start");
  }
  if (!Number.isFinite(intervalMs) || intervalMs < 10) {
    throw new Error("interval ms must be >= 10");
  }
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > AUTO_MAX_CONCURRENCY) {
    throw new Error(`concurrency must be an integer in [1, ${AUTO_MAX_CONCURRENCY}]`);
  }
  const userCount = userEnd - userStart + 1;
  if (userCount <= concurrency) {
    throw new Error("user_id range size must be greater than concurrency");
  }
  if (actions.length === 0) {
    throw new Error("enable at least one auto action");
  }
  void randomDecimalStringAtScale(priceMin, priceMax, pricePrecision);
  void randomDecimalStringAtScale(amountMin, amountMax, marketPrecision.stock);
  const normalizedRestingDepth = validatePositiveDecimalInput(
    restingDepth,
    marketPrecision.stock,
    "resting depth",
  );

  return {
    userStart,
    userEnd,
    intervalMs,
    concurrency,
    priceMin,
    priceMax,
    amountMin,
    amountMax,
    restingDepth: normalizedRestingDepth,
    actions,
  };
}

function firstWorkerUserId(
  cfg: AutoOrderConfig,
  workerIndex: number,
  workerCount = autoOrderState.workerCount || 1,
): number | null {
  void workerCount;
  const userId = cfg.userStart + workerIndex;
  return userId <= cfg.userEnd ? userId : null;
}

function nextAutoUserId(cfg: AutoOrderConfig, worker: AutoOrderWorkerState): number | null {
  const workerCount = Math.max(1, autoOrderState.workerCount);
  const firstUserId = firstWorkerUserId(cfg, worker.workerIndex, workerCount);
  if (firstUserId === null) {
    worker.nextUserId = null;
    return null;
  }

  if (
    worker.nextUserId === null ||
    worker.nextUserId < cfg.userStart ||
    worker.nextUserId > cfg.userEnd
  ) {
    worker.nextUserId = firstUserId;
  }

  const userId = worker.nextUserId;
  const nextUserId = userId + workerCount;
  worker.nextUserId = nextUserId <= cfg.userEnd ? nextUserId : firstUserId;
  return userId;
}

function workerLabel(worker: AutoOrderWorkerState): string {
  return `worker=${worker.workerIndex + 1}/${Math.max(1, autoOrderState.workerCount)}`;
}

function createAutoOrderWorkers(cfg: AutoOrderConfig): AutoOrderWorkerState[] {
  return Array.from({ length: cfg.concurrency }, (_, workerIndex) => ({
    workerIndex,
    timerId: undefined,
    correctionTimerId: undefined,
    nextUserId: firstWorkerUserId(cfg, workerIndex, cfg.concurrency),
  }));
}

function randomCorrectionDelayMs(): number {
  return (
    AUTO_CORRECTION_INTERVAL_MIN_MS +
    Math.floor(
      Math.random() * (AUTO_CORRECTION_INTERVAL_MAX_MS - AUTO_CORRECTION_INTERVAL_MIN_MS + 1),
    )
  );
}

function pickRandomAction(actions: AutoActionKind[]): AutoActionKind {
  return actions[Math.floor(Math.random() * actions.length)]!;
}

async function pickBalancedSide(): Promise<number> {
  try {
    const m = encodeURIComponent(marketName);
    const summary = await engineApi<MarketSummaryPayload>(`/markets/${m}/summary`);
    const askStock = parseNumberOrNull(summary.ask_stock_amount) ?? 0;
    const bidStock = parseNumberOrNull(summary.bid_stock_amount) ?? 0;
    const total = askStock + bidStock;
    if (total <= 0) return Math.random() < 0.5 ? 1 : 2;

    const imbalance = Math.abs(askStock - bidStock) / total;
    if (imbalance < AUTO_SIDE_IMBALANCE_THRESHOLD) return Math.random() < 0.5 ? 1 : 2;

    // More asks means the book needs more buys; more bids means it needs more sells.
    return askStock > bidStock ? 2 : 1;
  } catch {
    return Math.random() < 0.5 ? 1 : 2;
  }
}

async function submitLimitOrder(userId: number, side: number, price: string, amount: string): Promise<void> {
  await api<unknown>("/api/orders/limit", {
    method: "POST",
    body: JSON.stringify({
      user_id: userId,
      side,
      price: normalizePriceInput(price),
      amount: normalizeStockAmountInput(amount),
    }),
  });
}

async function submitMarketOrder(userId: number, side: number, amount: string): Promise<void> {
  await api<unknown>("/api/orders/market", {
    method: "POST",
    body: JSON.stringify({
      user_id: userId,
      side,
      amount: normalizeMarketAmountInput(side, amount),
    }),
  });
}

async function submitCancelOrder(userId: number, orderId: number): Promise<void> {
  await api<unknown>("/api/orders/cancel", {
    method: "POST",
    body: JSON.stringify({
      user_id: userId,
      order_id: orderId,
    }),
  });
}

async function fetchUserPendingOrders(userId: number): Promise<number[]> {
  const market = encodeURIComponent(marketName);
  const payload = await engineApi<UserOrdersPayload>(
    `/markets/${market}/users/${encodeURIComponent(String(userId))}/orders?offset=0&limit=50`,
  );
  const orders = Array.isArray(payload.orders) ? payload.orders : [];
  return orders
    .map((order) => Number(order.id))
    .filter((id) => Number.isFinite(id) && id > 0);
}

async function fetchOrderBookSide(side: number, limit = AUTO_ORDER_BOOK_DEPTH_LIMIT): Promise<Array<Record<string, unknown>>> {
  const market = encodeURIComponent(marketName);
  const payload = await engineApi<OrderBookPayload>(
    `/markets/${market}/order-book?side=${side}&offset=0&limit=${limit}`,
  );
  return Array.isArray(payload.orders) ? payload.orders : [];
}

function orderLeft(order: Record<string, unknown>): number {
  return parseNumberOrNull(order.left) ?? 0;
}

function orderPrice(order: Record<string, unknown>): number {
  return parseNumberOrNull(order.price) ?? 0;
}

function sumOrderLeft(orders: Array<Record<string, unknown>>): number {
  return orders.reduce((acc, order) => acc + Math.max(0, orderLeft(order)), 0);
}

function costToConsumeStock(orders: Array<Record<string, unknown>>, targetStock: number): number {
  let left = Math.max(0, targetStock);
  let cost = 0;
  for (const order of orders) {
    if (left <= 0) break;
    const amount = Math.min(left, Math.max(0, orderLeft(order)));
    const price = orderPrice(order);
    if (amount <= 0 || price <= 0) continue;
    cost += amount * price;
    left -= amount;
  }
  return cost;
}

async function buildMarketOrderAmount(
  side: number,
  targetStock: number,
  cfg: AutoOrderConfig,
): Promise<string | null> {
  const reserveStock = autoRestingDepthTarget(cfg);
  if (side === 2) {
    const asks = await fetchOrderBookSide(1);
    const availableStock = Math.max(0, sumOrderLeft(asks) - reserveStock);
    if (availableStock <= 0) return null;
    const amountMoney =
      costToConsumeStock(asks, Math.min(targetStock, availableStock)) * MARKET_BUY_AMOUNT_BUFFER;
    const formatted = amountMoney > 0 ? formatMoneyAmount(amountMoney) : null;
    return formatted !== null && parsePositiveAmount(formatted) > 0 ? formatted : null;
  }

  const bids = await fetchOrderBookSide(2);
  const availableStock = Math.max(0, sumOrderLeft(bids) - reserveStock);
  if (availableStock <= 0) return null;
  const formatted = formatStockAmount(Math.min(targetStock, availableStock));
  return parsePositiveAmount(formatted) > 0 ? formatted : null;
}

async function rebalanceOrderBook(worker: AutoOrderWorkerState): Promise<void> {
  if (!autoOrderState.running) return;
  const cfg = readAutoOrderConfig();
  const userId = nextAutoUserId(cfg, worker);
  if (userId === null) return;

  const [asks, bids] = await Promise.all([fetchOrderBookSide(1), fetchOrderBookSide(2)]);
  const askStock = sumOrderLeft(asks);
  const bidStock = sumOrderLeft(bids);
  const diff = askStock - bidStock;
  const targetStock = Math.abs(diff) * AUTO_CORRECTION_RATIO;
  if (targetStock <= 0) return;

  const side = diff > 0 ? 2 : 1;
  const amount = await buildMarketOrderAmount(side, targetStock, cfg);
  if (amount === null) {
    setAutoOrderStatus(`Correction: ${workerLabel(worker)} skip side=${side}, rival book too thin`);
    return;
  }

  setAutoOrderStatus(
    `Correction: ${workerLabel(worker)} market user=${userId} side=${side} amount=${amount}`,
  );
  await submitMarketOrder(userId, side, amount);
}

async function performAutoOrderStep(worker: AutoOrderWorkerState): Promise<void> {
  const cfg = readAutoOrderConfig();
  const userId = nextAutoUserId(cfg, worker);
  if (userId === null) {
    setAutoOrderStatus(`Running: ${workerLabel(worker)} has no assigned user_id`);
    return;
  }
  let action = pickRandomAction(cfg.actions);

  if (action === "cancel") {
    const orderIds = await fetchUserPendingOrders(userId);
    if (orderIds.length > 0) {
      const orderId = orderIds[Math.floor(Math.random() * orderIds.length)]!;
      setAutoOrderStatus(`Running: ${workerLabel(worker)} cancel user=${userId} order=${orderId}`);
      await submitCancelOrder(userId, orderId);
      return;
    }

    const fallback = cfg.actions.filter((item) => item !== "cancel");
    if (fallback.length === 0) {
      setAutoOrderStatus(`Running: ${workerLabel(worker)} no pending orders to cancel for user=${userId}`);
      return;
    }
    action = pickRandomAction(fallback);
  }

  const side = await pickBalancedSide();
  const amount = randomDecimalStringAtScale(cfg.amountMin, cfg.amountMax, marketPrecision.stock);

  if (action === "limit") {
    const price = randomDecimalStringAtScale(cfg.priceMin, cfg.priceMax, pricePrecision);
    setAutoOrderStatus(
      `Running: ${workerLabel(worker)} limit user=${userId} side=${side} price=${price} amount=${amount}`,
    );
    await submitLimitOrder(userId, side, price, amount);
    return;
  }

  const marketAmount = await buildMarketOrderAmount(side, parsePositiveAmount(amount), cfg);
  if (marketAmount === null) {
    setAutoOrderStatus(`Running: ${workerLabel(worker)} skip market side=${side}, rival book too thin`);
    return;
  }
  setAutoOrderStatus(
    `Running: ${workerLabel(worker)} market user=${userId} side=${side} amount=${marketAmount}`,
  );
  await submitMarketOrder(userId, side, marketAmount);
}

function clearAutoOrderTimer(worker: AutoOrderWorkerState): void {
  if (worker.timerId !== undefined) {
    window.clearTimeout(worker.timerId);
    worker.timerId = undefined;
  }
}

function clearAutoOrderBalanceTimer(worker: AutoOrderWorkerState): void {
  if (worker.correctionTimerId !== undefined) {
    window.clearTimeout(worker.correctionTimerId);
    worker.correctionTimerId = undefined;
  }
}

function clearAllAutoOrderTimers(): void {
  for (const worker of autoOrderState.workers) {
    clearAutoOrderTimer(worker);
    clearAutoOrderBalanceTimer(worker);
  }
}

function scheduleAutoOrderNextTick(worker: AutoOrderWorkerState, delayMs: number): void {
  clearAutoOrderTimer(worker);
  worker.timerId = window.setTimeout(() => {
    void runAutoOrderLoop(worker);
  }, delayMs);
}

function scheduleAutoOrderBalanceCheck(
  worker: AutoOrderWorkerState,
  delayMs = randomCorrectionDelayMs(),
): void {
  clearAutoOrderBalanceTimer(worker);
  worker.correctionTimerId = window.setTimeout(() => {
    void runAutoOrderBalanceLoop(worker);
  }, delayMs);
}

async function runAutoOrderLoop(worker: AutoOrderWorkerState): Promise<void> {
  if (!autoOrderState.running) return;
  let cfg: AutoOrderConfig;
  try {
    cfg = readAutoOrderConfig();
  } catch (e) {
    stopAutoOrder(e instanceof Error ? e.message : String(e));
    return;
  }

  try {
    await performAutoOrderStep(worker);
  } catch (e) {
    setAutoOrderStatus(
      `Auto order error: ${workerLabel(worker)} ${e instanceof Error ? e.message : String(e)}`,
    );
  }

  if (!autoOrderState.running) return;
  scheduleAutoOrderNextTick(worker, cfg.intervalMs);
}

async function runAutoOrderBalanceLoop(worker: AutoOrderWorkerState): Promise<void> {
  if (!autoOrderState.running) return;
  try {
    await rebalanceOrderBook(worker);
  } catch (e) {
    setAutoOrderStatus(
      `Auto correction error: ${workerLabel(worker)} ${e instanceof Error ? e.message : String(e)}`,
    );
  }
  if (!autoOrderState.running) return;
  scheduleAutoOrderBalanceCheck(worker);
}

function startAutoOrder(): void {
  const cfg = readAutoOrderConfig();
  autoOrderState.running = true;
  autoOrderState.workerCount = cfg.concurrency;
  autoOrderState.workers = createAutoOrderWorkers(cfg);
  updateAutoOrderButton();
  setAutoOrderStatus(`Starting ${autoOrderState.workerCount} workers...`);
  for (const worker of autoOrderState.workers) {
    scheduleAutoOrderNextTick(worker, 0);
    scheduleAutoOrderBalanceCheck(worker);
  }
}

function stopAutoOrder(status = "Stopped"): void {
  autoOrderState.running = false;
  clearAllAutoOrderTimers();
  autoOrderState.workerCount = 0;
  autoOrderState.workers = [];
  updateAutoOrderButton();
  setAutoOrderStatus(status);
}

function hasErrorMessage(j: InputProgressPayload): j is { error: string } {
  return typeof j.error === "string" && j.error.trim() !== "";
}

async function refreshInputProgress(): Promise<void> {
  const topicEl = document.getElementById("ob-input-topic");
  const highEl = document.getElementById("ob-kafka-high");
  const offEl = document.getElementById("ob-input-off");
  const lagEl = document.getElementById("ob-lag");
  const errEl = document.getElementById("ob-input-err");
  if (!topicEl || !highEl || !offEl || !lagEl) return;
  const m = encodeURIComponent(marketName);
  const fallbackTopic = `offer.${marketName}`;
  try {
    const j = await api<InputProgressPayload>(`/api/markets/${m}/input-progress`);
    if (hasErrorMessage(j)) {
      setText(topicEl, fallbackTopic, fallbackTopic);
      setText(highEl, "—");
      setText(offEl, "—");
      setText(lagEl, "—");
      lagEl.classList.remove("ip-lag--warn");
      setErrLine(errEl, j.error);
      return;
    }
    const topicDisp = fmtCell(j.topic);
    setText(topicEl, topicDisp !== "—" ? topicDisp : fallbackTopic, fallbackTopic);
    setText(highEl, j.kafka_high);
    setText(offEl, j.input_offset);
    setText(lagEl, j.lag);
    setErrLine(errEl, null);
    try {
      const lagStr = fmtCell(j.lag);
      if (lagStr !== "—" && BigInt(lagStr) > 0n) {
        lagEl.classList.add("ip-lag--warn");
      } else {
        lagEl.classList.remove("ip-lag--warn");
      }
    } catch {
      lagEl.classList.remove("ip-lag--warn");
    }
  } catch (e) {
    const msg =
      e == null
        ? "Network or API error (no error object)"
        : e instanceof Error
          ? e.message.trim() || e.name || "Error"
          : String(e);
    setText(topicEl, fallbackTopic, fallbackTopic);
    setText(highEl, "—");
    setText(offEl, "—");
    setText(lagEl, "—");
    lagEl.classList.remove("ip-lag--warn");
    setErrLine(
      errEl,
      msg === "null" || msg === "undefined" || msg === ""
        ? "Network or API error: ensure web-test is running on :8000 and /api/config is reachable."
        : msg,
    );
  }
}

async function refreshPublishPending(): Promise<void> {
  const quoteEl = document.getElementById("ob-publish-quote");
  const settleEl = document.getElementById("ob-publish-settle");
  const totalEl = document.getElementById("ob-publish-total");
  const groupsEl = document.getElementById("ob-publish-groups");
  const errEl = document.getElementById("ob-publish-err");
  if (!quoteEl || !settleEl || !totalEl || !groupsEl) return;

  const m = encodeURIComponent(marketName);
  try {
    const j = await engineApi<EngineStatusPayload>(`/markets/${m}/status`);
    if (typeof j.error === "string" && j.error.trim() !== "") {
      setText(quoteEl, "—");
      setText(settleEl, "—");
      setText(totalEl, "—");
      totalEl.classList.remove("ip-lag--warn");
      groupsEl.textContent = "—";
      setErrLine(errEl, j.error);
      return;
    }

    const dealsId = parseBigIntOrNull(j.deals_id);
    const pushedQuoteDealsId = parseBigIntOrNull(j.pushed_quote_deals_id);
    const settleMessageIds = parseBigIntArray(j.settle_message_ids);
    const pushedSettleMessageIds = parseBigIntArray(j.pushed_settle_message_ids);
    const settleTotal = sumIds(settleMessageIds);
    const pushedSettleTotal = sumIds(pushedSettleMessageIds);
    const quoteLag = positiveDiff(dealsId, pushedQuoteDealsId);
    const settleGroupCount = Math.max(settleMessageIds.length, pushedSettleMessageIds.length);
    const settleLag = Array.from({ length: settleGroupCount }, (_, idx) =>
      positiveDiff(settleMessageIds[idx] ?? 0n, pushedSettleMessageIds[idx] ?? 0n),
    ).reduce((acc, lag) => acc + lag, 0n);
    const totalLag = quoteLag + settleLag;

    setText(quoteEl, formatProgress(pushedQuoteDealsId, dealsId));
    setText(settleEl, `${pushedSettleTotal}/${settleTotal}`);
    setText(totalEl, totalLag);
    setErrLine(errEl, null);

    if (totalLag > 0n) {
      totalEl.classList.add("ip-lag--warn");
    } else {
      totalEl.classList.remove("ip-lag--warn");
    }

    const groupItems = Array.from({ length: settleGroupCount }, (_, idx) => {
      const current = settleMessageIds[idx] ?? 0n;
      const pushed = pushedSettleMessageIds[idx] ?? 0n;
      return {
        groupId: String(idx),
        current,
        pushed,
        lag: positiveDiff(current, pushed),
      };
    })
      .filter((item) => item.current > 0n || item.pushed > 0n)
      .map((item) => ({
        groupId: item.groupId,
        progress: `${item.pushed}/${item.current}`,
        lag: String(item.lag),
      }))

    if (groupItems.length === 0) {
      groupsEl.textContent = "—";
      return;
    }

    groupsEl.innerHTML = groupItems
      .map((item) => {
        const level = classifyPendingLevel(item.lag);
        return `<code class="ip-group-chip ip-group-chip--${level}">settle.${escapeHtml(item.groupId)}=${escapeHtml(item.progress)}</code>`;
      })
      .join("");
  } catch (e) {
    const msg =
      e == null
        ? "Network or API error (no error object)"
        : e instanceof Error
          ? e.message.trim() || e.name || "Error"
          : String(e);
    setText(quoteEl, "—");
    setText(settleEl, "—");
    setText(totalEl, "—");
    totalEl.classList.remove("ip-lag--warn");
    groupsEl.textContent = "—";
    setErrLine(
      errEl,
      msg === "null" || msg === "undefined" || msg === ""
        ? "Network or API error: ensure web-test is running on :8000 and /api/config is reachable."
        : msg,
    );
  }
}

async function refreshOrderbook(): Promise<void> {
  const limit = Number((document.getElementById("ob-limit") as HTMLInputElement).value || 12);
  const offset = Math.max(
    0,
    Math.floor(Number((document.getElementById("ob-offset") as HTMLInputElement).value || 0)),
  );
  const m = encodeURIComponent(marketName);
  try {
    const [summary, asks, bids] = await Promise.all([
      engineApi<unknown>(`/markets/${m}/summary`),
      engineApi<unknown>(
        `/markets/${m}/order-book?side=1&offset=${offset}&limit=${limit}`,
      ),
      engineApi<unknown>(
        `/markets/${m}/order-book?side=2&offset=${offset}&limit=${limit}`,
      ),
    ]);

    show($("out-ob-overview"), summary);
    const asksBody = document.getElementById("asksBody") as HTMLTableSectionElement;
    const bidsBody = document.getElementById("bidsBody") as HTMLTableSectionElement;
    renderOrderRows(asksBody, asks, undefined, true);
    renderOrderRows(bidsBody, bids);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    show($("out-ob-overview"), msg);
    const asksBody = document.getElementById("asksBody") as HTMLTableSectionElement;
    const bidsBody = document.getElementById("bidsBody") as HTMLTableSectionElement;
    renderOrderRows(asksBody, null, msg, true);
    renderOrderRows(bidsBody, null, msg);
  }
}

function clearHttpRefreshTimer(): void {
  if (httpRefreshState.timerId !== undefined) {
    window.clearTimeout(httpRefreshState.timerId);
    httpRefreshState.timerId = undefined;
  }
}

function formatRefreshTime(d: Date): string {
  return d.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function classifyPendingLevel(pending: string): string {
  try {
    const n = BigInt(pending);
    if (n <= 0n) return "zero";
    if (n < 5n) return "low";
    if (n < 20n) return "medium";
    return "high";
  } catch {
    return "unknown";
  }
}

function getRefreshIntervalInput(): HTMLInputElement | null {
  return document.getElementById("refresh-interval-ms") as HTMLInputElement | null;
}

function normalizeHttpRefreshInterval(raw: unknown): number {
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n)) return DEFAULT_HTTP_REFRESH_MS;
  return Math.max(MIN_HTTP_REFRESH_MS, n);
}

function getHttpRefreshInterval(): number {
  const input = getRefreshIntervalInput();
  if (!input) return DEFAULT_HTTP_REFRESH_MS;
  const intervalMs = normalizeHttpRefreshInterval(input.value);
  if (String(intervalMs) !== input.value) {
    input.value = String(intervalMs);
  }
  return intervalMs;
}

function setRunningStateButton(btn: HTMLButtonElement, running: boolean): void {
  btn.classList.toggle("state-toggle-btn--running", running);
  btn.classList.toggle("state-toggle-btn--idle", !running);
}

function updateRefreshControls(statusOverride?: string): void {
  const refreshBtn = document.getElementById("btn-refresh-now") as HTMLButtonElement | null;
  const autoBtn = document.getElementById("btn-auto-refresh") as HTMLButtonElement | null;
  const statusEl = document.getElementById("refresh-status");
  const intervalInput = getRefreshIntervalInput();
  if (!refreshBtn || !autoBtn || !statusEl || !intervalInput) return;
  const intervalMs = getHttpRefreshInterval();

  refreshBtn.disabled = httpRefreshState.inFlight;
  refreshBtn.textContent = httpRefreshState.inFlight ? "Refreshing..." : "Refresh";
  autoBtn.textContent = httpRefreshState.enabled ? "Auto refresh: On" : "Auto refresh: Off";
  setRunningStateButton(autoBtn, httpRefreshState.enabled);
  intervalInput.min = String(MIN_HTTP_REFRESH_MS);

  if (statusOverride !== undefined) {
    statusEl.textContent = statusOverride;
    return;
  }

  statusEl.textContent = httpRefreshState.enabled
    ? `Auto refresh every ${intervalMs}ms`
    : lastHttpRefreshText;
}

function scheduleHttpAutoRefresh(delayMs = getHttpRefreshInterval()): void {
  clearHttpRefreshTimer();
  if (!httpRefreshState.enabled) return;
  httpRefreshState.timerId = window.setTimeout(() => {
    void triggerHttpRefresh("auto");
  }, delayMs);
}

async function refreshHttpDataSections(): Promise<void> {
  await Promise.all([
    refreshEngineStatus(),
    refreshOrderbook(),
    refreshInputProgress(),
    refreshPublishPending(),
  ]);
}

async function triggerHttpRefresh(reason: "manual" | "auto" | "init"): Promise<void> {
  if (httpRefreshState.inFlight) {
    httpRefreshState.queued = true;
    return;
  }

  clearHttpRefreshTimer();
  httpRefreshState.inFlight = true;
  updateRefreshControls("Refreshing...");

  try {
    await refreshHttpDataSections();
    lastHttpRefreshText = `Last refresh ${formatRefreshTime(new Date())}`;
    updateRefreshControls(
      lastHttpRefreshText,
    );
  } finally {
    httpRefreshState.inFlight = false;

    if (httpRefreshState.queued) {
      httpRefreshState.queued = false;
      void triggerHttpRefresh(reason);
      return;
    }

    if (httpRefreshState.enabled) {
      scheduleHttpAutoRefresh();
    } else {
      updateRefreshControls();
    }
  }
}

function startHttpAutoRefresh(): void {
  if (httpRefreshState.enabled) return;
  httpRefreshState.enabled = true;
  updateRefreshControls();
  void triggerHttpRefresh("auto");
}

function stopHttpAutoRefresh(): void {
  httpRefreshState.enabled = false;
  httpRefreshState.queued = false;
  clearHttpRefreshTimer();
  updateRefreshControls();
}

document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    const id = (btn as HTMLElement).dataset.tab as ManualTabKind | undefined;
    if (!id) return;
    activeManualTab = id;
    document.getElementById("form-limit")!.classList.toggle("hidden", id !== "limit");
    document.getElementById("form-market")!.classList.toggle("hidden", id !== "market");
    document.getElementById("form-cancel")!.classList.toggle("hidden", id !== "cancel");
    updateManualSubmitButton();
  });
});

document.getElementById("btn-manual-submit")!.addEventListener("click", () => {
  const formId =
    activeManualTab === "limit"
      ? "form-limit"
      : activeManualTab === "market"
        ? "form-market"
        : "form-cancel";
  const form = document.getElementById(formId) as HTMLFormElement | null;
  form?.requestSubmit();
});

document.getElementById("form-limit")!.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = formJson(ev.target as HTMLFormElement);
  try {
    await submitLimitOrder(Number(f.user_id), Number(f.side), String(f.price), String(f.amount));
  } catch (e) {
    console.error(e);
  }
});

document.getElementById("form-market")!.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = formJson(ev.target as HTMLFormElement);
  try {
    await submitMarketOrder(Number(f.user_id), Number(f.side), String(f.amount));
  } catch (e) {
    console.error(e);
  }
});

document.getElementById("form-cancel")!.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = formJson(ev.target as HTMLFormElement);
  try {
    await submitCancelOrder(Number(f.user_id), Number(f.order_id));
  } catch (e) {
    console.error(e);
  }
});

document.getElementById("btn-auto-order")!.addEventListener("click", () => {
  if (autoOrderState.running) {
    stopAutoOrder();
    return;
  }
  try {
    startAutoOrder();
  } catch (e) {
    setAutoOrderStatus(`Auto order config error: ${e instanceof Error ? e.message : String(e)}`);
  }
});

document.getElementById("btn-refresh-now")!.addEventListener("click", () => {
  void triggerHttpRefresh("manual");
});

document.getElementById("btn-auto-refresh")!.addEventListener("click", () => {
  if (httpRefreshState.enabled) {
    stopHttpAutoRefresh();
    return;
  }
  startHttpAutoRefresh();
});

document.getElementById("refresh-interval-ms")!.addEventListener("change", () => {
  void getHttpRefreshInterval();
  if (httpRefreshState.enabled) {
    scheduleHttpAutoRefresh();
  }
  updateRefreshControls(lastHttpRefreshText);
});

const wsProto = location.protocol === "https:" ? "wss:" : "ws:";
const wsUrl = `${wsProto}//${location.host}/ws`;
const wsOutSettle = $("out-ws-settle");
const wsOutQuote = $("out-ws-quote");
const maxLines = 150;
const linesSettle: string[] = [];
const linesQuote: string[] = [];

/** Real-time feed (WebSocket settle / quote); default on, user can disconnect from header. */
let wsLivePushEnabled = true;
let ws: WebSocket | null = null;

function renderWsPanels(): void {
  wsOutSettle.textContent = linesSettle.join("\n");
  wsOutQuote.textContent = linesQuote.join("\n");
}

function updateWsLivePushButton(): void {
  const btn = document.getElementById("btn-ws-live-push") as HTMLButtonElement | null;
  if (!btn) return;
  btn.textContent = wsLivePushEnabled ? "Real-time feed: On" : "Real-time feed: Off";
  setRunningStateButton(btn, wsLivePushEnabled);
}

type WsEnvelope = { channel?: string; data?: unknown };

function handleWsMessage(ev: MessageEvent): void {
  const raw = ev.data as string;
  let target: "settle" | "quote" = "settle";
  let line = raw;
  try {
    const msg = JSON.parse(raw) as WsEnvelope;
    if (msg.channel === "quote") {
      target = "quote";
    } else if (msg.channel === "settle") {
      target = "settle";
    }
    line = typeof msg.data === "object" && msg.data !== null ? JSON.stringify(msg.data) : raw;
  } catch {
    /* non-JSON payload: show raw in settle line for debugging */
  }
  if (target === "quote") {
    linesQuote.push(line);
    while (linesQuote.length > maxLines) linesQuote.shift();
  } else {
    linesSettle.push(line);
    while (linesSettle.length > maxLines) linesSettle.shift();
  }
  renderWsPanels();
}

function connectWebSocketIfNeeded(): void {
  if (!wsLivePushEnabled) return;
  if (ws !== null && (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN)) {
    return;
  }
  const socket = new WebSocket(wsUrl);
  socket.addEventListener("message", handleWsMessage);
  socket.addEventListener("close", () => {
    if (ws === socket) {
      ws = null;
    }
    if (wsLivePushEnabled) {
      connectWebSocketIfNeeded();
    }
  });
  ws = socket;
}

function disconnectWebSocket(): void {
  if (!ws) return;
  const prev = ws;
  ws.removeEventListener("message", handleWsMessage);
  ws = null;
  try {
    prev.close();
  } catch {
    /* ignore */
  }
}

function setWsLivePushEnabled(on: boolean): void {
  wsLivePushEnabled = on;
  updateWsLivePushButton();
  if (on) {
    connectWebSocketIfNeeded();
  } else {
    disconnectWebSocket();
  }
}

document.getElementById("btn-ws-live-push")!.addEventListener("click", () => {
  setWsLivePushEnabled(!wsLivePushEnabled);
});

document.getElementById("btn-ws-clear")!.addEventListener("click", () => {
  linesSettle.length = 0;
  linesQuote.length = 0;
  renderWsPanels();
});

void (async () => {
  await loadConfig();
  updateManualSubmitButton();
  updateAutoOrderButton();
  setAutoOrderStatus("Idle");
  updateWsLivePushButton();
  connectWebSocketIfNeeded();
  startHttpAutoRefresh();
})();
