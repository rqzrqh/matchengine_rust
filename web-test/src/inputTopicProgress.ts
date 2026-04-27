import type { Kafka } from "kafkajs";
import { engineGet } from "./engineClient.js";

let admin: ReturnType<Kafka["admin"]> | null = null;
let connectOnce: Promise<void> | null = null;

/**
 * Matches `src/main.rs`: input messages go to `offer.<market>` partition 0.
 * Lag vs `input_offset` from engine `market.status`.
 */
export function bindKafkaForInputProgress(kafka: Kafka): void {
  if (admin) {
    return;
  }
  admin = kafka.admin();
  connectOnce = admin.connect();
}

async function waitAdmin(): Promise<NonNullable<typeof admin>> {
  if (!connectOnce || !admin) {
    throw new Error("bindKafkaForInputProgress was not called");
  }
  await connectOnce;
  return admin;
}

export type InputProgressResponse =
  | {
      topic: string;
      partition: number;
      kafka_high: string;
      input_offset: string;
      lag: string;
    }
  | { error: string };

export async function getInputTopicProgress(marketName: string): Promise<InputProgressResponse> {
  const topic = `offer.${marketName}`;
  try {
    const a = await waitAdmin();
    const offs = await a.fetchTopicOffsets(topic);
    const p0 = offs.find((x) => x.partition === 0);
    if (!p0) {
      return { error: `partition 0 not found for ${topic}` };
    }
    const rawHigh = p0.high ?? p0.offset;
    if (rawHigh === undefined || rawHigh === null || String(rawHigh).trim() === "") {
      return { error: `${topic} partition 0 has no valid high/offset` };
    }
    const high = BigInt(String(rawHigh));
    const { status, text } = await engineGet(
      `/markets/${encodeURIComponent(marketName)}/status`,
    );
    if (status !== 200) {
      return { error: `engine /status ${status}: ${text.slice(0, 300)}` };
    }
    const st = JSON.parse(text) as { input_offset?: number | string };
    const raw = st.input_offset;
    if (raw === undefined || raw === null) {
      return { error: "market.status has no input_offset" };
    }
    const inOff = BigInt(String(raw));
    const next = inOff < 0n ? 0n : inOff + 1n;
    const lag = high >= next ? high - next : 0n;
    return {
      topic,
      partition: 0,
      kafka_high: high.toString(),
      input_offset: inOff.toString(),
      lag: lag.toString(),
    };
  } catch (e) {
    if (e == null) {
      return { error: "unknown error" };
    }
    if (e instanceof Error) {
      const m = (e.message || e.name).trim() || "error";
      return { error: m === "null" || m === "undefined" ? "unknown error" : m };
    }
    const s = String(e).trim();
    if (s === "null" || s === "undefined" || s === "") {
      return { error: "unknown error" };
    }
    return { error: s };
  }
}
