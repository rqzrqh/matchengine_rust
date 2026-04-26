import fs from "fs";
import path from "path";
import yaml from "js-yaml";

/** Same shape as repo root `config.yaml` (shared by engine and web-test: market, brokers, db). */
export interface MatchengineYaml {
  market: {
    name: string;
    stock_prec: number;
    money_prec: number;
    fee_prec: number;
    min_amount: string;
  };
  brokers: string;
  db: {
    addr: string;
    user: string;
    passwd: string;
  };
  /** Engine-only; optional for web-test. */
  snap_cleanup?: {
    max_age_secs: number;
    max_snapshots: number;
    cleanup_interval_secs: number;
  };
  snap_dump?: {
    dump_interval_secs: number;
  };
}

/** Default engine REST URL, web console public URL, and HTTP listen port. */
export const ENGINE_PUBLIC_HTTP_URL = "http://127.0.0.1:8080";
export const WEB_APP_PUBLIC_URL = "http://127.0.0.1:8000";
export const WEB_BACKEND_LISTEN_PORT = 8000;

/** Override engine REST base (server-side proxy target). Prefer `127.0.0.1` if engine binds IPv4 only. */
export function resolveEngineHttpUrl(): string {
  const raw = process.env.ENGINE_HTTP_URL?.trim() || process.env.MATCENGINE_ENGINE_HTTP_URL?.trim();
  const base = raw && raw.length > 0 ? raw : ENGINE_PUBLIC_HTTP_URL;
  return base.replace(/\/$/, "");
}

/** Engine uses database name = `market.name` */
export function buildMysqlDatabaseUrl(yaml: MatchengineYaml, databaseName: string): string {
  const user = encodeURIComponent(yaml.db.user);
  const pass = encodeURIComponent(yaml.db.passwd);
  return `mysql://${user}:${pass}@${yaml.db.addr}/${databaseName}`;
}

export type KafkaAutoOffsetReset = "earliest" | "latest";

export interface AppResolvedConfig {
  yaml: MatchengineYaml;
  configPath: string;
  configDir: string;
  databaseUrl: string;
  mysqlSummary: string;
  engineUrl: string;
  kafkaBootstrap: string;
  marketName: string;
  clientDistDir: string;
  userSettleGroupSize: number;
  kafkaAutoOffsetReset: KafkaAutoOffsetReset;
}

let app: AppResolvedConfig | null = null;

export function findConfigPath(): string {
  const candidates = [
    path.resolve(process.cwd(), "config.yaml"),
    path.resolve(process.cwd(), "..", "config.yaml"),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) return p;
  }
  throw new Error("config.yaml not found: place it at repo root or start from web-test/");
}

export function loadYamlFile(configPath: string): MatchengineYaml {
  const raw = fs.readFileSync(configPath, "utf8");
  return yaml.load(raw) as MatchengineYaml;
}

export function initAppConfig(yaml: MatchengineYaml, configPathAbs: string): AppResolvedConfig {
  const configDir = path.dirname(configPathAbs);
  const marketName = yaml.market.name;
  const databaseUrl = buildMysqlDatabaseUrl(yaml, marketName);
  const consolePkgRoot = path.join(configDir, "web-test");
  const clientDistDir = path.join(consolePkgRoot, "dist", "client");
  const userSettleGroupSize = 64;
  const mysqlSummary = `mysql://${yaml.db.addr}/${marketName}`;
  const kafkaAutoOffsetReset: KafkaAutoOffsetReset =
    process.env.KAFKA_AUTO_OFFSET_RESET === "earliest" ? "earliest" : "latest";

  app = {
    yaml,
    configPath: configPathAbs,
    configDir,
    databaseUrl,
    mysqlSummary,
    engineUrl: resolveEngineHttpUrl(),
    kafkaBootstrap: yaml.brokers,
    marketName,
    clientDistDir,
    userSettleGroupSize,
    kafkaAutoOffsetReset,
  };
  return app;
}

export function getAppConfig(): AppResolvedConfig {
  if (!app) {
    throw new Error("initAppConfig was not called");
  }
  return app;
}
