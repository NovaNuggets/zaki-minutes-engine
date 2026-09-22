/**
 * L3 — lifecycle-http adapter (HTTP callback transport). OFFLINE, NO network.
 *
 * Injects a fake `fetchImpl` that records every call and asserts:
 *   • the POST hits callbackUrl with `content-type: application/json` + MeetingToken bearer auth,
 *     and the body is the lifecycle.v1 event JSON verbatim (0.11 convention);
 *   • `token` omitted → no `authorization` header;
 *   • a transient failure (network throw + non-2xx) is RETRIED with backoff, then succeeds;
 *   • a permanent failure does NOT throw out of `emit` (the bot must not crash).
 * Run: npx tsx src/adapters/lifecycle-http.test.ts
 */
import { createHttpLifecycleSink, type FetchLike } from './lifecycle-http.js';
import type { LifecycleEvent } from '../contracts.js';

let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond ? '' : '  — ' + detail}`);
  if (!cond) failed++;
};

interface Recorded {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string;
  redirect: 'error';
  signal: AbortSignal;
}
const noSleep = async (): Promise<void> => {};

const EVENT: LifecycleEvent = {
  connection_id: 'sess-uid',
  status: 'completed',
  completion_reason: 'stopped',
  exit_code: 0,
};

async function main(): Promise<void> {
  // ── happy: one POST, correct url + headers + verbatim body ──
  {
    const calls: Recorded[] = [];
    const fetchImpl: FetchLike = async (url, init) => { calls.push({ url, ...init }); return { ok: true, status: 200 }; };
    const sink = createHttpLifecycleSink({ callbackUrl: 'http://meeting-api:8080/bots/internal/callback/lifecycle', token: 'MEETING-TOKEN', fetchImpl, sleep: noSleep });
    await sink.emit(EVENT);
    check('happy: exactly one POST', calls.length === 1, String(calls.length));
    check('happy: hits callbackUrl', calls[0]?.url === 'http://meeting-api:8080/bots/internal/callback/lifecycle', calls[0]?.url);
    check('happy: method POST', calls[0]?.method === 'POST', calls[0]?.method);
    check('happy: redirects are refused', calls[0]?.redirect === 'error', calls[0]?.redirect);
    check('happy: content-type json', calls[0]?.headers['content-type'] === 'application/json', JSON.stringify(calls[0]?.headers));
    check('happy: MeetingToken bearer header', calls[0]?.headers.authorization === 'Bearer MEETING-TOKEN', JSON.stringify(calls[0]?.headers));
    check('happy: platform internal header absent', !('x-internal-secret' in (calls[0]?.headers ?? {})), JSON.stringify(calls[0]?.headers));
    check('happy: body is the lifecycle.v1 event verbatim', calls[0]?.body === JSON.stringify(EVENT), calls[0]?.body);
    check('happy: body round-trips to the event', JSON.stringify(JSON.parse(calls[0]!.body)) === JSON.stringify(EVENT));
  }

  // ── response disposal + wall-clock timeout are bounded ──
  {
    let cancelled = 0;
    const fetchImpl: FetchLike = async () => ({
      ok: true,
      status: 200,
      body: { cancel: async () => { cancelled++; } },
    });
    await createHttpLifecycleSink({ callbackUrl: 'http://cb', fetchImpl, sleep: noSleep }).emit(EVENT);
    check('response body is cancelled without buffering', cancelled === 1, String(cancelled));

    let aborted = false;
    const hanging: FetchLike = async (_url, init) => new Promise((_resolve, reject) => {
      init.signal.addEventListener('abort', () => {
        aborted = true;
        reject(new Error('aborted'));
      }, { once: true });
    });
    await createHttpLifecycleSink({
      callbackUrl: 'http://cb', fetchImpl: hanging, retries: 1, timeoutMs: 1, sleep: noSleep,
    }).emit(EVENT);
    check('hung callback is aborted at the per-attempt deadline', aborted);
  }

  // ── no token → no authorization header ──
  {
    const calls: Recorded[] = [];
    const fetchImpl: FetchLike = async (url, init) => { calls.push({ url, ...init }); return { ok: true, status: 204 }; };
    const sink = createHttpLifecycleSink({ callbackUrl: 'http://cb', fetchImpl, sleep: noSleep });
    await sink.emit(EVENT);
    check('no-token: authorization absent', !('authorization' in (calls[0]?.headers ?? {})), JSON.stringify(calls[0]?.headers));
    check('no-secret: 204 counts as success (single attempt)', calls.length === 1, String(calls.length));
  }

  // ── transient: throw, then 500, then 200 — retried with backoff, succeeds on attempt 3 ──
  {
    const calls: Recorded[] = [];
    const sleeps: number[] = [];
    let n = 0;
    const fetchImpl: FetchLike = async (url, init) => {
      calls.push({ url, ...init });
      n++;
      if (n === 1) throw new Error('ECONNREFUSED');
      if (n === 2) return { ok: false, status: 500 };
      return { ok: true, status: 200 };
    };
    const sink = createHttpLifecycleSink({ callbackUrl: 'http://cb', fetchImpl, retries: 3, backoffMs: 100, sleep: async (ms) => { sleeps.push(ms); } });
    await sink.emit(EVENT);
    check('retry: three attempts before success', calls.length === 3, String(calls.length));
    check('retry: backoff was bounded + exponential (100, 200)', JSON.stringify(sleeps) === JSON.stringify([100, 200]), JSON.stringify(sleeps));
  }

  // ── permanent failure: every attempt throws → emit MUST NOT throw, stops after `retries` ──
  {
    const calls: Recorded[] = [];
    const fetchImpl: FetchLike = async (url, init) => { calls.push({ url, ...init }); throw new Error('network down'); };
    const sink = createHttpLifecycleSink({ callbackUrl: 'http://cb', fetchImpl, retries: 3, sleep: noSleep });
    let threw = false;
    try { await sink.emit(EVENT); } catch { threw = true; }
    check('permanent: emit did NOT throw (bot does not crash)', threw === false);
    check('permanent: gave up after exactly `retries` attempts', calls.length === 3, String(calls.length));
  }

  // ── permanent non-2xx: every attempt 503 → also swallowed, bounded attempts ──
  {
    const calls: Recorded[] = [];
    const fetchImpl: FetchLike = async (url, init) => { calls.push({ url, ...init }); return { ok: false, status: 503 }; };
    const sink = createHttpLifecycleSink({ callbackUrl: 'http://cb', fetchImpl, retries: 2, sleep: noSleep });
    let threw = false;
    try { await sink.emit(EVENT); } catch { threw = true; }
    check('permanent-5xx: emit did NOT throw', threw === false);
    check('permanent-5xx: bounded to `retries` attempts', calls.length === 2, String(calls.length));
  }

  if (failed) { console.error(`\n❌ lifecycle-http (L3): ${failed} check(s) FAILED.`); process.exit(1); }
  console.log('\n✅ lifecycle-http (L3): POSTs lifecycle.v1 with the MeetingToken bearer, retries with bounded backoff, and never throws out of emit.');
}

void main();
