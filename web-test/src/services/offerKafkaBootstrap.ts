import { randomUUID } from "crypto";
import type { Kafka } from "kafkajs";
import { getAppConfig } from "../config.js";
import { getDb } from "../db.js";
import { OfferDispatchState, OfferOrder } from "../db/entities.js";
import { unixSecondsNow } from "../time.js";

const PARTITION = 0;
const BOOTSTRAP_RUN_MS = 45_000;

/** Kafka JSON may use `input_sequence_id` (engine wire format) or `sequence_id`. */
function parseSequenceIdFromPayload(raw: string): number {
  let body: Record<string, unknown>;
  try {
    body = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    throw new Error("invalid JSON");
  }
  const rawSeq = body.sequence_id ?? body.input_sequence_id;
  let seq: number;
  if (typeof rawSeq === "number") seq = rawSeq;
  else if (typeof rawSeq === "string") seq = Number(rawSeq.trim());
  else if (rawSeq != null) seq = Number(rawSeq);
  else seq = NaN;
  if (!Number.isFinite(seq) || seq < 1 || !Number.isInteger(seq)) {
    const preview = raw.length > 240 ? `${raw.slice(0, 240)}…` : raw;
    throw new Error(`missing or invalid sequence_id / input_sequence_id (preview=${preview})`);
  }
  return seq;
}

async function applyKafkaParsedSequence(seq: number): Promise<void> {
  const mgr = getDb().manager;
  const now = unixSecondsNow();
  await mgr.update(
    OfferDispatchState,
    { id: 1 },
    {
      lastSequenceId: seq,
      lastSentSequenceId: seq,
      updatedAt: now,
    },
  );

  const row = await mgr.findOne(OfferDispatchState, { where: { id: 1 } });
  if (!row || row.lastSequenceId !== seq || row.lastSentSequenceId !== seq) {
    throw new Error(
      `offer_dispatch_state verify failed: expected last_sequence_id=last_sent_sequence_id=${seq}, got last_sequence_id=${row?.lastSequenceId ?? "null"} last_sent_sequence_id=${row?.lastSentSequenceId ?? "null"}`,
    );
  }
}

/**
 * If `offer_orders` is empty, read the latest `offer.<market>` p0 message and set both
 * `last_sequence_id` and `last_sent_sequence_id` from its `sequence_id` / `input_sequence_id`.
 *
 * Uses `eachBatch` + deferred `seek` after `GROUP_JOIN`; subscribes from beginning as
 * a fallback so bootstrap still reaches the target if KafkaJS ignores an early seek.
 */
export async function bootstrapOfferLastSequenceFromKafka(kafka: Kafka): Promise<void> {
  const mgr = getDb().manager;

  const dispatchRow = await mgr.findOne(OfferDispatchState, { where: { id: 1 } });
  if (!dispatchRow) {
    console.error("[offer-bootstrap] offer_dispatch_state id=1 missing; skip Kafka bootstrap");
    return;
  }

  const cnt = await mgr.count(OfferOrder);
  if (cnt > 0) {
    console.log(
      `[offer-bootstrap] offer_orders has ${cnt} row(s); skip Kafka bootstrap (sequence columns unchanged)`,
    );
    return;
  }

  const cfg = getAppConfig();
  const topic = `offer.${cfg.marketName}`;

  const admin = kafka.admin();
  await admin.connect();
  let offs;
  try {
    offs = await admin.fetchTopicOffsets(topic);
  } catch (e) {
    console.error("[offer-bootstrap] fetchTopicOffsets failed:", e);
    return;
  } finally {
    await admin.disconnect().catch(() => {});
  }

  const p0 = offs.find((x) => x.partition === PARTITION);
  const highStr = p0?.high ?? p0?.offset;
  const lowStr = p0?.low ?? "0";
  if (highStr === undefined || highStr === null || String(highStr).trim() === "") {
    console.warn("[offer-bootstrap] could not read offer topic offsets; skip");
    return;
  }

  const high = BigInt(String(highStr));
  const low = BigInt(String(lowStr));
  if (high <= 0n) {
    console.log("[offer-bootstrap] offer topic empty (high<=0); skip");
    return;
  }

  let targetOffset = high - 1n;
  if (targetOffset < low) targetOffset = low;

  console.log(
    `[offer-bootstrap] topic=${topic} p${PARTITION} low=${lowStr} high=${String(highStr)} seek=${targetOffset.toString()}`,
  );

  const consumer = kafka.consumer({
    groupId: `web-offer-bootstrap-${cfg.marketName}-${randomUUID()}`,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    maxWaitTimeInMs: 4000,
    minBytes: 1,
  });

  let seekScheduled = false;
  const scheduleSeek = () => {
    if (seekScheduled || !consumer) return;
    seekScheduled = true;
    queueMicrotask(() => {
      try {
        consumer.seek({
          topic,
          partition: PARTITION,
          offset: targetOffset.toString(),
        });
      } catch (e) {
        console.error("[offer-bootstrap] seek failed", e);
      }
    });
  };

  consumer.on(consumer.events.GROUP_JOIN, scheduleSeek);

  let handled = false;
  let finished = false;
  let finish: () => void = () => {};
  const done = new Promise<void>((resolve) => {
    finish = () => {
      if (finished) return;
      finished = true;
      resolve();
    };
  });
  let timeoutId: ReturnType<typeof setTimeout> | undefined;

  try {
    await consumer.connect();
    await consumer.subscribe({ topics: [topic], fromBeginning: true });

    timeoutId = setTimeout(() => {
      if (!handled) {
        console.error(
          `[offer-bootstrap] timed out after ${BOOTSTRAP_RUN_MS}ms (no target batch); sequence columns not updated`,
        );
        finish();
      }
    }, BOOTSTRAP_RUN_MS);

    await consumer.run({
      autoCommit: false,
      eachBatch: async ({ batch, heartbeat, pause }) => {
        await heartbeat();
        if (handled) return;
        if (batch.messages.length === 0) return;

        const eligible = batch.messages.filter((m) => BigInt(m.offset) >= targetOffset);
        if (eligible.length === 0) {
          return;
        }

        handled = true;
        if (timeoutId !== undefined) clearTimeout(timeoutId);
        pause();

        const lastMsg = eligible[eligible.length - 1]!;
        const raw = lastMsg.value?.toString() ?? "{}";

        try {
          const seq = parseSequenceIdFromPayload(raw);
          await applyKafkaParsedSequence(seq);
          console.log(
            `[offer-bootstrap] last_sequence_id=${seq} last_sent_sequence_id=${seq} (from Kafka offset=${lastMsg.offset})`,
          );
        } catch (e) {
          console.error("[offer-bootstrap] could not apply bootstrap:", e);
        } finally {
          finish();
        }
      },
    });

    await done;
  } catch (e) {
    console.error("[offer-bootstrap] consumer failed:", e);
    finish();
  } finally {
    if (timeoutId !== undefined) clearTimeout(timeoutId);
    await consumer.disconnect().catch(() => {});
  }
}
