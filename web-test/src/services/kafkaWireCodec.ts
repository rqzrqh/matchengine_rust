import { decode } from "@msgpack/msgpack";

export type JsonWireValue =
  | null
  | boolean
  | number
  | string
  | JsonWireValue[]
  | { [key: string]: JsonWireValue };

export type JsonWireObject = { [key: string]: JsonWireValue };

export function decodeMsgpackObject(raw: Buffer, context: string): Record<string, unknown> {
  const decoded = decode(raw);
  if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) {
    throw new Error(`${context}: invalid MessagePack object`);
  }
  return decoded as Record<string, unknown>;
}

export function normalizeForJson(value: unknown): JsonWireValue {
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean" || typeof value === "string") return value;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "bigint") return value.toString();
  if (Array.isArray(value)) return value.map(normalizeForJson);
  if (typeof value === "object") {
    const out: JsonWireObject = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = normalizeForJson(v);
    }
    return out;
  }
  return String(value);
}

export function normalizeObjectForJson(value: Record<string, unknown>): JsonWireObject {
  return normalizeForJson(value) as JsonWireObject;
}
