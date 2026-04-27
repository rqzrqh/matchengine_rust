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

/** Interval for all polled HTTP requests on this page (engine status, order book). */
const HTTP_POLL_MS = 500;

let marketName = __MATCENGINE_CONFIG__.market.name;
let engineHttpBase = DEFAULT_ENGINE_HTTP;

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
  const el = $("out-status");
  try {
    const market_status = await engineApi<unknown>(
      `/markets/${encodeURIComponent(marketName)}/status`,
    );
    show(el, market_status);
  } catch (e) {
    show(el, { error: e instanceof Error ? e.message : String(e) });
  }
}

type InputProgressPayload = {
  topic?: string | null;
  kafka_high?: string | null;
  input_offset?: string | null;
  lag?: string | null;
  error?: string | null;
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

document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    const id = (btn as HTMLElement).dataset.tab;
    document.getElementById("form-limit")!.classList.toggle("hidden", id !== "limit");
    document.getElementById("form-market")!.classList.toggle("hidden", id !== "market");
    document.getElementById("form-cancel")!.classList.toggle("hidden", id !== "cancel");
  });
});

document.getElementById("form-limit")!.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = formJson(ev.target as HTMLFormElement);
  try {
    await api<unknown>("/api/orders/limit", {
      method: "POST",
      body: JSON.stringify({
        user_id: Number(f.user_id),
        side: Number(f.side),
        price: String(f.price),
        amount: String(f.amount),
      }),
    });
  } catch (e) {
    console.error(e);
  }
});

document.getElementById("form-market")!.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = formJson(ev.target as HTMLFormElement);
  try {
    await api<unknown>("/api/orders/market", {
      method: "POST",
      body: JSON.stringify({
        user_id: Number(f.user_id),
        side: Number(f.side),
        amount: String(f.amount),
      }),
    });
  } catch (e) {
    console.error(e);
  }
});

document.getElementById("form-cancel")!.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const f = formJson(ev.target as HTMLFormElement);
  try {
    await api<unknown>("/api/orders/cancel", {
      method: "POST",
      body: JSON.stringify({
        user_id: Number(f.user_id),
        order_id: Number(f.order_id),
      }),
    });
  } catch (e) {
    console.error(e);
  }
});

const wsProto = location.protocol === "https:" ? "wss:" : "ws:";
const ws = new WebSocket(`${wsProto}//${location.host}/ws`);
const wsOutSettle = $("out-ws-settle");
const wsOutQuote = $("out-ws-quote");
const maxLines = 150;
const linesSettle: string[] = [];
const linesQuote: string[] = [];

function renderWsPanels(): void {
  wsOutSettle.textContent = linesSettle.join("\n");
  wsOutQuote.textContent = linesQuote.join("\n");
}

type WsEnvelope = { channel?: string; data?: unknown };

ws.addEventListener("message", (ev) => {
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
});

document.getElementById("btn-ws-clear")!.addEventListener("click", () => {
  linesSettle.length = 0;
  linesQuote.length = 0;
  renderWsPanels();
});

void (async () => {
  await loadConfig();
  void refreshEngineStatus();
  setInterval(() => {
    void refreshEngineStatus();
  }, HTTP_POLL_MS);
  void refreshOrderbook();
  setInterval(() => {
    void refreshOrderbook();
  }, HTTP_POLL_MS);
  void refreshInputProgress();
  setInterval(() => {
    void refreshInputProgress();
  }, HTTP_POLL_MS);
})();
