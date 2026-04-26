import path from "node:path";
import { fileURLToPath } from "node:url";
import type { MySql2Database } from "drizzle-orm/mysql2";
import { drizzle } from "drizzle-orm/mysql2";
import { migrate } from "drizzle-orm/mysql2/migrator";
import mysql from "mysql2/promise";
import * as schema from "./db/schema.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
/** `web-test/` root (parent of `src` or `dist`) */
export const webRoot = path.join(__dirname, "..");

export type AppDb = MySql2Database<typeof schema>;

let pool: mysql.Pool | null = null;
let db: AppDb | null = null;

/** Run Drizzle migrations and create the pool (same MySQL URL as config.yaml). */
export async function setupDatabase(databaseUrl: string): Promise<AppDb> {
  process.env.DATABASE_URL = databaseUrl;

  const migrationConn = await mysql.createConnection(databaseUrl);
  const migrationDb = drizzle(migrationConn);
  await migrate(migrationDb, { migrationsFolder: path.join(webRoot, "drizzle") });
  await migrationConn.end();

  pool = mysql.createPool(databaseUrl);
  db = drizzle(pool, { schema, mode: "default" });
  return db;
}

export function getDb(): AppDb {
  if (!db) {
    throw new Error("setupDatabase was not called");
  }
  return db;
}

export async function disconnectDb(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
    db = null;
  }
}
