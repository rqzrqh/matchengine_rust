/**
 * web-test entry: Express + Vite (dev), MySQL via TypeORM, Kafka producers/consumers, WebSocket fan-out.
 */
import "reflect-metadata";
import { randomUUID } from "crypto";
import path from "node:path";
import fs from "node:fs";
import { createServer, type Server } from "http";
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { WebSocketServer, WebSocket } from "ws";
import { Kafka } from "kafkajs";
import {
  findConfigPath,
  initAppConfig,
  loadYamlFile,
  getAppConfig,
  WEB_APP_PUBLIC_URL,
  WEB_BACKEND_LISTEN_PORT,
} from "./config.js";
import { setupDatabase, disconnectDb, webRoot } from "./db.js";
import { FeedHub } from "./services/feedHub.js";
import { bootstrapOfferLastSequenceFromKafka } from "./services/offerKafkaBootstrap.js";
import { OfferDispatchService } from "./services/offerDispatchService.js";
import { OfferSequenceService } from "./services/offerSequenceService.js";
import { OfferService } from "./services/offerService.js";
import { OfferTopicMonitor } from "./services/offerTopicMonitor.js";
import { QuoteService } from "./services/quoteService.js";
import { SettleService } from "./services/settleService.js";
import { registerRoutes } from "./routes/registerRoutes.js";
import { bindKafkaForInputProgress } from "./inputTopicProgress.js";

dotenv.config();

const RESTART_DELAY_MS = 3000;

const configPath = findConfigPath();
const yaml = loadYamlFile(configPath);
initAppConfig(yaml, configPath);

const clientRoot = path.join(webRoot, "client");
const isDev = process.env.NODE_ENV !== "production";

function logFatalError(context: string, err: unknown): void {
  console.error(`[web-test] ${context}`);
  if (err instanceof Error) {
    console.error(err.message);
    if (err.stack) console.error(err.stack);
  } else {
    console.error(err);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * One full run: listen until SIGINT/SIGTERM, then tear down and resolve the promise.
 * On listen failure, reject; outer loop logs and restarts after 3s.
 */
async function runSession(): Promise<void> {
  await setupDatabase(getAppConfig().databaseUrl);

  const kafka = new Kafka({
    clientId: `matchengine-web-test-${randomUUID()}`,
    brokers: getAppConfig().kafkaBootstrap.split(",").map((s) => s.trim()),
  });
  bindKafkaForInputProgress(kafka);

  const producer = kafka.producer();
  const hub = new FeedHub();
  const offerDispatch = new OfferDispatchService(producer);
  const offerSequence = new OfferSequenceService();
  const offerService = new OfferService(offerSequence);
  const offerTopicMonitor = new OfferTopicMonitor(kafka);
  const quoteService = new QuoteService(kafka, hub);
  const settleService = new SettleService(kafka, hub);

  let server: Server | null = null;
  let wss: WebSocketServer | null = null;

  const cleanupResources = async (): Promise<void> => {
    try {
      await quoteService.stop();
    } catch {
      /* ignore */
    }
    try {
      await settleService.stop();
    } catch {
      /* ignore */
    }
    try {
      await offerDispatch.stop();
    } catch {
      /* ignore */
    }
    try {
      await offerSequence.stop();
    } catch {
      /* ignore */
    }
    try {
      await offerTopicMonitor.stop();
    } catch {
      /* ignore */
    }
    try {
      await producer.disconnect();
    } catch {
      /* ignore */
    }
    try {
      await disconnectDb();
    } catch {
      /* ignore */
    }
    await new Promise<void>((resolve) => {
      if (wss) {
        wss.close(() => resolve());
      } else {
        resolve();
      }
    });
    await new Promise<void>((resolve) => {
      if (server && server.listening) {
        server.close(() => resolve());
      } else {
        resolve();
      }
    });
  };

  try {
    await producer.connect();
    await bootstrapOfferLastSequenceFromKafka(kafka);
    offerSequence.start();
    offerDispatch.start();

    void quoteService.start().catch((e) => {
      console.error("quote service consumer failed", e);
    });
    void settleService.start().catch((e) => {
      console.error("settle service consumer failed", e);
    });
    void offerTopicMonitor.start().catch((e) => {
      console.error("[offer-topic-monitor] failed", e);
    });

    const app = express();
    app.use(cors());
    app.use(express.json({ limit: "1mb" }));

    registerRoutes(app, offerService, offerTopicMonitor);

    if (isDev) {
      const { createServer: createViteServer } = await import("vite");
      const vite = await createViteServer({
        root: clientRoot,
        server: { middlewareMode: true },
        appType: "spa",
      });
      app.use(vite.middlewares);
    } else {
      const dist = getAppConfig().clientDistDir;
      app.use(express.static(dist));
      app.get("*", (_req, res, next) => {
        const index = path.join(dist, "index.html");
        if (fs.existsSync(index)) {
          res.sendFile(index);
        } else {
          next();
        }
      });
    }

    const httpServer = createServer(app);
    server = httpServer;
    wss = new WebSocketServer({ server: httpServer, path: "/ws" });
    wss.on("connection", (ws: WebSocket) => {
      hub.add(ws);
      ws.on("close", () => hub.remove(ws));
      ws.on("error", () => hub.remove(ws));
    });

    await new Promise<void>((resolveSession, rejectSession) => {
      let settled = false;

      const teardown = async (mode: "ok" | "err", err?: Error): Promise<void> => {
        if (settled) return;
        settled = true;
        process.off("SIGINT", onSig);
        process.off("SIGTERM", onSig);
        httpServer.off("error", onServerError);
        try {
          await cleanupResources();
        } finally {
          if (mode === "ok") resolveSession();
          else rejectSession(err ?? new Error("server error"));
        }
      };

      function onSig(): void {
        void teardown("ok");
      }

      const onServerError = (e: Error): void => {
        void teardown("err", e);
      };

      process.once("SIGINT", onSig);
      process.once("SIGTERM", onSig);
      httpServer.once("error", onServerError);

      httpServer.listen(WEB_BACKEND_LISTEN_PORT, () => {
        console.log(`Web HTTP listening at ${WEB_APP_PUBLIC_URL}/`);
      });
    });
  } catch (e) {
    await cleanupResources();
    throw e;
  }
}

async function runWithRestart(): Promise<void> {
  for (;;) {
    try {
      await runSession();
      console.log("[web-test] stopped cleanly");
      process.exit(0);
    } catch (err) {
      logFatalError(`Process error, restarting in ${RESTART_DELAY_MS / 1000}s`, err);
      await sleep(RESTART_DELAY_MS);
    }
  }
}

runWithRestart().catch((e) => {
  logFatalError("runWithRestart failed", e);
  process.exit(1);
});
