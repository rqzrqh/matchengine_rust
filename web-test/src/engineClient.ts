import { getAppConfig } from "./config.js";

function engineBase(): string {
  return getAppConfig().engineUrl.replace(/\/$/, "");
}

/** GET a path on the engine (path must start with `/`). */
export async function engineGet(path: string): Promise<{ status: number; text: string }> {
  const r = await fetch(`${engineBase()}${path.startsWith("/") ? path : `/${path}`}`, {
    method: "GET",
    signal: AbortSignal.timeout(30_000),
  });
  const text = await r.text();
  return { status: r.status, text };
}
