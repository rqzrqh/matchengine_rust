import "./style.css";

interface ApiConfig {
  market_name: string;
  engine_url?: string;
}

interface ApiErrorBody {
  detail?: string;
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
    const body = data as ApiErrorBody & { error?: string };
    const msg =
      typeof body?.detail === "string"
        ? body.detail
        : typeof body?.error === "string"
          ? body.error
          : typeof data === "string"
            ? data
            : JSON.stringify(data);
    throw new Error(msg);
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
    const body = data as ApiErrorBody & { error?: string };
    const msg =
      typeof body?.detail === "string"
        ? body.detail
        : typeof body?.error === "string"
          ? body.error
          : typeof data === "string"
            ? data
            : JSON.stringify(data);
    throw new Error(msg);
  }
  return data as T;
}

function show(el: HTMLElement, obj: unknown): void {
  el.textContent = typeof obj === "string" ? obj : JSON.stringify(obj, null, 2);
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
    show($("out-asks"), asks);
    show($("out-bids"), bids);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    show($("out-ob-overview"), msg);
    show($("out-asks"), msg);
    show($("out-bids"), msg);
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
})();
