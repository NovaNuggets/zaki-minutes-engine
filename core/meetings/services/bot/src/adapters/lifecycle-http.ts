/**
 * lifecycle.v1 egress ADAPTER — HTTP callback to meeting-api.
 *
 * Implements the `LifecycleSink` port by POSTing each lifecycle.v1 event verbatim to the
 * meeting-api callback URL (`inv.meetingApiCallbackUrl`):
 *   • headers: `content-type: application/json` + the per-spawn MeetingToken bearer
 *   • body: the lifecycle.v1 event JSON, as-is (no envelope)
 *
 * L3-testable via an INJECTED `fetchImpl` (defaults to Node 22's native `fetch` — NO new dep).
 * The composition root builds the live adapter; the test injects a recording/failing fake.
 *
 * Robustness (P14): a lifecycle POST failure must NEVER crash the bot. `emit` retries with a
 * bounded backoff on a network error or a non-2xx response, then logs + gives up — it never
 * throws fatally out of `emit`. (A dropped status report is regrettable but must not strand a
 * seated bot or mask the terminal exit.)
 */
import type { LifecycleEvent } from '../contracts.js';
import type { LifecycleSink } from '../ports.js';

/** The minimal fetch shape we depend on (a subset of the WHATWG `fetch`), so the test can
 *  inject a fake without pulling in DOM/undici types. */
export type FetchLike = (
  url: string,
  init: {
    method: string;
    headers: Record<string, string>;
    body: string;
    redirect: 'error';
    signal: AbortSignal;
  },
) => Promise<{
  ok: boolean;
  status: number;
  body?: { cancel(): Promise<void> | void } | null;
}>;

export interface HttpLifecycleSinkOptions {
  /** meeting-api's lifecycle.v1 callback URL (invocation.v1 `meetingApiCallbackUrl`). */
  callbackUrl: string;
  /** Per-spawn MeetingToken — never a platform-wide service credential. */
  token?: string;
  /** Injected for the L3 test; defaults to Node 22's native global `fetch`. */
  fetchImpl?: FetchLike;
  /** Max POST attempts (1 try + retries). Default 3. */
  retries?: number;
  /** Base backoff (ms) between attempts; doubles each retry (bounded). Default 200ms. */
  backoffMs?: number;
  /** Sleep impl (injected so the test runs instantly). Default real setTimeout. */
  sleep?: (ms: number) => Promise<void>;
  /** Per-attempt wall-clock deadline. Default 5 seconds. */
  timeoutMs?: number;
}

const realSleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Build the live HTTP lifecycle sink. `emit` POSTs the event with bounded retry/backoff and
 *  never throws — a permanent failure is logged and swallowed (the bot keeps running). */
export function createHttpLifecycleSink(opts: HttpLifecycleSinkOptions): LifecycleSink {
  const {
    callbackUrl,
    token,
    fetchImpl = globalThis.fetch as unknown as FetchLike,
    retries = 3,
    backoffMs = 200,
    sleep = realSleep,
    timeoutMs = 5_000,
  } = opts;

  const headers: Record<string, string> = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;

  const attempts = Math.max(1, retries);

  async function emit(event: LifecycleEvent): Promise<void> {
    const body = JSON.stringify(event);
    let lastErr: string | undefined;
    for (let attempt = 1; attempt <= attempts; attempt++) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const res = await fetchImpl(callbackUrl, {
          method: 'POST', headers, body, redirect: 'error', signal: controller.signal,
        });
        try {
          await res.body?.cancel();
        } catch {
          /* best-effort bounded response disposal */
        }
        if (res.ok) return; // 2xx — delivered
        lastErr = `HTTP ${res.status}`;
      } catch (e) {
        lastErr = (e as Error)?.message ?? String(e);
      } finally {
        clearTimeout(timer);
      }
      // Bounded exponential backoff before the next attempt (none after the last).
      if (attempt < attempts) await sleep(backoffMs * 2 ** (attempt - 1));
    }
    // Give up — log, never throw (a lifecycle POST failure must not crash the bot, P14).
    console.error(
      `[bot] lifecycle.v1 ${event.status} POST failed after ${attempts} attempt(s): ${lastErr ?? 'unknown'} (giving up)`,
    );
  }

  return { emit };
}
