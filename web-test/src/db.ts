import path from "node:path";
import { fileURLToPath } from "node:url";
import { DataSource } from "typeorm";
import {
  OfferDispatchState,
  OfferOrder,
  QuoteConsumerState,
  QuoteDealTick,
  SettleConsumerState,
  SettleMessage,
} from "./db/entities.js";
import { unixSecondsNow } from "./time.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
/** `web-test/` root (parent of `src` or `dist`) */
export const webRoot = path.join(__dirname, "..");

export type AppDb = DataSource;

let ds: DataSource | null = null;

async function seedOfferDispatchState(dataSource: DataSource): Promise<void> {
  const now = unixSecondsNow();
  await dataSource.query(
    `INSERT IGNORE INTO offer_dispatch_state (id, last_sequence_id, last_sent_sequence_id, updated_at) VALUES (?, ?, ?, ?)`,
    [1, 0, 0, now],
  );
}

/**
 * TypeORM `synchronize`: creates/updates tables for registered entities only.
 * Other tables already in MySQL are left untouched (no Drizzle Kit rename prompts).
 */
export async function setupDatabase(databaseUrl: string): Promise<DataSource> {
  process.env.DATABASE_URL = databaseUrl;

  const dataSource = new DataSource({
    type: "mysql",
    url: databaseUrl,
    entities: [
      QuoteDealTick,
      QuoteConsumerState,
      SettleMessage,
      SettleConsumerState,
      OfferOrder,
      OfferDispatchState,
    ],
    synchronize: true,
    logging: false,
  });

  await dataSource.initialize();
  await seedOfferDispatchState(dataSource);

  ds = dataSource;
  return dataSource;
}

export function getDb(): DataSource {
  if (!ds) {
    throw new Error("setupDatabase was not called");
  }
  return ds;
}

export async function disconnectDb(): Promise<void> {
  if (ds) {
    await ds.destroy();
    ds = null;
  }
}
