/**
 * Express routes: `GET /api/config`, engine proxy paths, offer APIs, and DB-backed tick/history feeds.
 */
import type { Express, Request, Response } from "express";
import { desc } from "drizzle-orm";
import { engineGet } from "../engineClient.js";
import { getAppConfig } from "../config.js";
import { getDb } from "../db.js";
import { quoteDealTicks, settleMessages } from "../db/schema.js";
import type { OfferService } from "../services/offerService.js";
import type { OfferTopicMonitor } from "../services/offerTopicMonitor.js";
import { serializeQuoteDealTick } from "../services/quoteService.js";
import { serializeSettleMessage } from "../services/settleService.js";
import { getInputTopicProgress } from "../inputTopicProgress.js";

export function registerRoutes(
  app: Express,
  offerService: OfferService,
  offerTopicMonitor: OfferTopicMonitor,
): void {
  app.get("/api/config", (_req: Request, res: Response) => {
    const cfg = getAppConfig();
    res.json({
      engine_url: cfg.engineUrl,
      market_name: cfg.marketName,
      mysql_summary: cfg.mysqlSummary,
      kafka_bootstrap: cfg.kafkaBootstrap,
    });
  });

  app.get("/api/markets/:market/summary", async (req: Request, res: Response) => {
    try {
      const { status, text } = await engineGet(
        `/markets/${encodeURIComponent(req.params.market)}/summary`,
      );
      res.status(status).type("application/json").send(text);
    } catch (e) {
      res.status(502).json({ error: String(e) });
    }
  });

  app.get("/api/markets/:market/status", async (req: Request, res: Response) => {
    try {
      const { status, text } = await engineGet(
        `/markets/${encodeURIComponent(req.params.market)}/status`,
      );
      res.status(status).type("application/json").send(text);
    } catch (e) {
      res.status(502).json({ error: String(e) });
    }
  });

  app.get("/api/markets/:market/publish-pending", async (req: Request, res: Response) => {
    try {
      const { status, text } = await engineGet(
        `/markets/${encodeURIComponent(req.params.market)}/publish-pending`,
      );
      res.status(status).type("application/json").send(text);
    } catch (e) {
      res.status(502).json({ error: String(e) });
    }
  });

  /**
   * Offer topic: {@link OfferTopicMonitor} reads partition high watermark via Admin on each request
   * (no consumer; browser drives refresh via this route).
   */
  app.get("/api/markets/:market/offer-topic/end-offset", async (req: Request, res: Response) => {
    const cfg = getAppConfig();
    if (req.params.market !== cfg.marketName) {
      res.status(404).json({ error: "unknown market" });
      return;
    }
    try {
      res.json(await offerTopicMonitor.getSnapshot());
    } catch (e) {
      res.status(500).json({ error: String(e) });
    }
  });

  /** Input topic high vs `market.status.input_offset` (approx. backlog in messages). */
  app.get("/api/markets/:market/input-progress", async (req: Request, res: Response) => {
    try {
      const out = await getInputTopicProgress(req.params.market);
      if ("error" in out) {
        res.status(200).json(out);
        return;
      }
      res.json(out);
    } catch (e) {
      res.status(500).json({ error: String(e) });
    }
  });

  app.get("/api/markets/:market/orders/:orderId", async (req: Request, res: Response) => {
    try {
      const { status, text } = await engineGet(
        `/markets/${encodeURIComponent(req.params.market)}/orders/${encodeURIComponent(req.params.orderId)}`,
      );
      res.status(status).type("application/json").send(text);
    } catch (e) {
      res.status(502).json({ error: String(e) });
    }
  });

  app.get("/api/markets/:market/orderbook", async (req: Request, res: Response) => {
    try {
      const q = new URLSearchParams();
      if (req.query.side !== undefined) q.set("side", String(req.query.side));
      if (req.query.offset !== undefined) q.set("offset", String(req.query.offset));
      if (req.query.limit !== undefined) q.set("limit", String(req.query.limit));
      const qs = q.toString();
      const path = `/markets/${encodeURIComponent(req.params.market)}/order-book${qs ? `?${qs}` : ""}`;
      const { status, text } = await engineGet(path);
      res.status(status).type("application/json").send(text);
    } catch (e) {
      res.status(502).json({ error: String(e) });
    }
  });

  app.get("/api/markets/:market/users/:userId/orders", async (req: Request, res: Response) => {
    try {
      const q = new URLSearchParams();
      if (req.query.offset !== undefined) q.set("offset", String(req.query.offset));
      if (req.query.limit !== undefined) q.set("limit", String(req.query.limit));
      const qs = q.toString();
      const path = `/markets/${encodeURIComponent(req.params.market)}/users/${encodeURIComponent(req.params.userId)}/orders${qs ? `?${qs}` : ""}`;
      const { status, text } = await engineGet(path);
      res.status(status).type("application/json").send(text);
    } catch (e) {
      res.status(502).json({ error: String(e) });
    }
  });

  app.get("/api/orders", async (_req: Request, res: Response) => {
    try {
      res.json({ orders: await offerService.listOrders() });
    } catch (e) {
      res.status(500).json({ error: String(e) });
    }
  });

  app.post("/api/orders/limit", async (req: Request, res: Response) => {
    try {
      res.json(await offerService.placeLimit(req.body as Record<string, unknown>));
    } catch (e) {
      res.status(500).json({ error: String(e) });
    }
  });

  app.post("/api/orders/market", async (req: Request, res: Response) => {
    try {
      res.json(await offerService.placeMarket(req.body as Record<string, unknown>));
    } catch (e) {
      res.status(500).json({ error: String(e) });
    }
  });

  app.post("/api/orders/cancel", async (req: Request, res: Response) => {
    try {
      res.json(await offerService.cancel(req.body as Record<string, unknown>));
    } catch (e) {
      res.status(500).json({ error: String(e) });
    }
  });

  app.get("/api/quote_ticks", async (req: Request, res: Response) => {
    const limit = Math.min(500, Math.max(1, Number(req.query.limit ?? 200) || 200));
    try {
      const db = getDb();
      const rows = await db
        .select()
        .from(quoteDealTicks)
        .orderBy(desc(quoteDealTicks.id))
        .limit(limit);
      res.json({ rows: rows.map(serializeQuoteDealTick) });
    } catch (e) {
      res.status(500).json({ error: String(e) });
    }
  });

  app.get("/api/settle_messages", async (req: Request, res: Response) => {
    const limit = Math.min(500, Math.max(1, Number(req.query.limit ?? 200) || 200));
    try {
      const db = getDb();
      const rows = await db
        .select()
        .from(settleMessages)
        .orderBy(desc(settleMessages.id))
        .limit(limit);
      res.json({ rows: rows.map(serializeSettleMessage) });
    } catch (e) {
      res.status(500).json({ error: String(e) });
    }
  });
}
