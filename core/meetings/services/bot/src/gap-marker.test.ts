/**
 * L3 (L-0270) — the TRANSCRIPT GAP MARKER. OFFLINE, NO browser/whisper/redis.
 *
 * The incident (staging meeting 49, 2026-09-15): the STT provider returned HTTP 503
 * for 14 minutes. The client retried 4× per chunk, threw, and the lane dropped the
 * audio. The stored transcript had NO hole in it — the missing minutes were simply
 * absent, the summary read as complete, and nothing told the reader that part of the
 * meeting was never transcribed.
 *
 * This pins the truth leg on the REAL gmeet lane (the lane meeting 49 ran on) through
 * the bot's createBotPipeline, both directions of the truth:
 *   A  an outage is ONE coalesced, spanned, speaker-neutral marker, logged once, and the
 *      fault reaches the host's onError once per failure (not again per lost span);
 *   A2 the marker and the warn line are CONTENT-FREE: no provider text, not even through
 *      a foreign fault's `kind` (read contract §3);
 *   B  a lost turn is marked when the TURN closes (not only at session end), and a marker
 *      never merges across transcribed speech (gap · text · gap = TWO markers);
 *   C  a fault the stream recovers from inside the same turn is NOT a gap;
 *   D  an outage longer than the buffer cap loses the audio the cap discards — marked for
 *      exactly that span even though the stream recovers afterwards;
 *   E  an outage the buffer still holds when the STT recovers is re-heard — no marker.
 *
 * Run: npx tsx src/gap-marker.test.ts
 */
import Ajv2020, { type ValidateFunction } from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createBotPipeline, type BotPipeline } from './pipeline.js';
import type { Invocation } from './config.js';
import type { TranscriptSegment } from './contracts.js';
import type { TranscriptSink } from './ports.js';
import { TranscriptionError, type TranscriptionResult } from '@vexa/transcribe-whisper';

let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond ? '' : '  — ' + detail}`);
  if (!cond) failed++;
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ── transcript.v1 validator (ajv against the PUBLISHED schema; same pattern as pipeline.test.ts) ──
const HERE = dirname(fileURLToPath(import.meta.url));
const TX_SCHEMA = join(HERE, '..', '..', '..', 'contracts', 'transcript.v1', 'transcript.schema.json');
const txSchema = JSON.parse(readFileSync(TX_SCHEMA, 'utf8'));
const ajv = new Ajv2020({ strict: false, allErrors: true });
addFormats(ajv);
ajv.addSchema(txSchema);
const validateSeg: ValidateFunction = ajv.compile({ $ref: `${txSchema.$id}#/$defs/TranscriptSegment` });

function captureSink(): TranscriptSink & { readonly published: TranscriptSegment[] } {
  const published: TranscriptSegment[] = [];
  return { published, async publish(seg) { published.push(seg); } };
}
const gapsOf = (s: TranscriptSegment[]) => s.filter((x) => x.segment_id.startsWith('gap:'));
const lastById = (s: TranscriptSegment[]) => [...new Map(s.map((x) => [x.segment_id, x])).values()];
const spokenOf = (s: TranscriptSegment[]) => s.filter((x) => !x.segment_id.startsWith('gap:') && (x.text ?? '').trim());

const baseInv = (over: Partial<Invocation> = {}): Invocation => ({
  platform: 'google_meet', meetingUrl: 'https://meet.google.com/abc-defg-hij', botName: 'Vexa',
  redisUrl: 'redis://localhost:6379', transcribeEnabled: true, ...over,
});

const SR = 16000;
const FRAME_MS = 200;
// Fast lane config: confirm in ~hundreds of ms, and a 5 s buffer cap (production: 30 s).
const FAST = { minAudioDuration: 0.15, submitInterval: 0.1, confirmThreshold: 2, maxBufferDuration: 5, idleTimeoutSec: 2, sampleRate: SR };
/** The incident's wall clock (09:03:50Z, the first 503 on meeting 49). */
const T0 = Date.UTC(2026, 8, 15, 9, 3, 50);
/** A provider body that must never reach the meeting record or the warn line. */
const SENTINEL = 'SENTINEL-PROVIDER-BODY';

const HEARD = 0.06;
const LOST = 0.05;
const frame = (amp: number) => new Float32Array((SR * FRAME_MS) / 1000).fill(amp);
const ok = (): TranscriptionResult =>
  ({ text: 'hello world', language: 'en', duration: 0.2, segments: [{ start: 0, end: 0.2, text: 'hello world' }] });

/** Feed one contiguous channel-0 turn of `frames` frames from `startTs`. Returns the next ts. */
async function feedTurn(pipe: BotPipeline, startTs: number, frames: number, amp = LOST, onFrame?: (i: number) => void): Promise<number> {
  let ts = startTs;
  const f = frame(amp);
  for (let i = 0; i < frames; i++) { onFrame?.(i); pipe.feedAudio(0, 'Alice', f, ts); ts += FRAME_MS; await sleep(110); }
  return ts;
}

/** Capture console.warn for the duration of `fn`. */
async function withWarn<T>(fn: (warned: string[]) => Promise<T>): Promise<T> {
  const warned: string[] = [];
  const realWarn = console.warn;
  console.warn = (...a: unknown[]) => { warned.push(a.map(String).join(' ')); };
  try { return await fn(warned); } finally { console.warn = realWarn; }
}

async function scenarioA(): Promise<void> {
  console.log('\n[A] an outage across two turns → one coalesced, spanned, content-free marker');
  await withWarn(async (warned) => {
    const sink = captureSink();
    let failures = 0;
    const hostFaults: unknown[] = [];
    const transcribe = async (): Promise<never> => {
      failures++;
      throw new TranscriptionError('unavailable', 503, SENTINEL, true);
    };
    const pipe = createBotPipeline(baseInv(), sink, { transcribe, config: FAST, onError: (e) => hostFaults.push(e) });
    await pipe.start();
    const afterFirst = await feedTurn(pipe, T0, 12);
    await feedTurn(pipe, afterFirst + 3000, 12);
    await sleep(300);
    await pipe.stop();

    const gaps = gapsOf(sink.published);
    const last = gaps[gaps.length - 1];
    const ids = new Set(gaps.map((s) => s.segment_id));
    check('the dropped audio produced a GAP MARKER segment', gaps.length >= 1, JSON.stringify(sink.published));
    check('no phantom transcript text was published', spokenOf(sink.published).length === 0);
    check('contiguous losses COALESCE into ONE marker (not one per dropped window)', ids.size === 1, [...ids].join(', '));
    check('the marker is a completed segment (survives reload like any other)', last?.completed === true);
    check('the marker is speaker-neutral (no person is credited with the silence)', last?.speaker === 'system', last?.speaker);
    check('the marker text names the covered span and the provider reason',
      !!last && /transcription unavailable/i.test(last.text) && last.text.includes('09:03:50') && last.text.includes('HTTP 503'),
      last?.text);
    check('the marker SPAN starts at the lost audio (epoch seconds)', !!last && Math.abs(last.start - T0 / 1000) < 1.5, `${last?.start}`);
    check('the marker SPAN was EXTENDED over the whole outage (both turns)', !!last && last.end - last.start >= 7,
      `${(last ? last.end - last.start : 0).toFixed(1)}s`);
    check('the marker carries absolute times (the reader sees when)',
      !!last?.absolute_start_time && Math.abs(new Date(last.absolute_start_time).getTime() / 1000 - last.start) < 1);
    check('every published segment is transcript.v1-valid (ajv vs SSOT)',
      sink.published.length > 0 && sink.published.every((s) => !!validateSeg(s)), ajv.errorsText(validateSeg.errors));
    check('the gap is logged ONCE at warn with the span (not one line per attempt)',
      warned.filter((w) => /gap/i.test(w)).length === 1, JSON.stringify(warned));
    check('CONTENT-FREE: the provider body is in no stored marker text',
      !sink.published.some((s) => (s.text ?? '').includes(SENTINEL)), JSON.stringify(gaps.map((g) => g.text)));
    check('CONTENT-FREE: the provider body is in no warn line', !warned.some((w) => w.includes(SENTINEL)), JSON.stringify(warned));
    check('the host sees each STT fault ONCE (a lost-span report is not a second fault)',
      hostFaults.length === failures, `host=${hostFaults.length} failures=${failures}`);
  });
}

async function scenarioA2(): Promise<void> {
  console.log('\n[A2] a foreign fault object cannot smuggle text in through `kind`');
  await withWarn(async (warned) => {
    const sink = captureSink();
    const transcribe = async (): Promise<never> => {
      throw Object.assign(new Error(`${SENTINEL} message`), { kind: `${SENTINEL} kind text`, status: 503 });
    };
    const pipe = createBotPipeline(baseInv(), sink, { transcribe, config: FAST });
    await pipe.start();
    await feedTurn(pipe, T0, 12);
    await sleep(300);
    await pipe.stop();
    const marker = gapsOf(sink.published).pop();
    check('a marker is still written', !!marker, JSON.stringify(sink.published));
    check('CONTENT-FREE: a non-enum `kind` never reaches the marker text',
      !!marker && !marker.text.includes(SENTINEL) && marker.text.includes('provider unavailable (HTTP 503)'), marker?.text);
    check('CONTENT-FREE: … nor the warn line', !warned.some((w) => w.includes(SENTINEL)), JSON.stringify(warned));
  });
}

async function scenarioB(): Promise<void> {
  console.log('\n[B] gap · text · gap on one channel → TWO markers, each written when its turn closes');
  await withWarn(async () => {
    const sink = captureSink();
    // The provider fails exactly the LOST turns' audio (by content), so each turn's own close-flush
    // fails or succeeds with it — the truth does not depend on wall-clock timing.
    const transcribe = async (pcm: Float32Array): Promise<TranscriptionResult> => {
      if (Math.abs(pcm[0] - LOST) < 0.005) throw new TranscriptionError('unavailable', 503, undefined, true);
      return ok();
    };
    const pipe = createBotPipeline(baseInv(), sink, { transcribe, config: FAST });
    await pipe.start();

    const t1End = await feedTurn(pipe, T0, 12, LOST);                  // turn 1: lost
    const t2Start = t1End + 3000;
    const t2End = await feedTurn(pipe, t2Start, 12, HEARD);            // turn 2: heard (closes turn 1)
    await sleep(400);
    const firstBeforeStop = lastById(gapsOf(sink.published));
    check('turn 1 is MARKED when it closes — before pipe.stop() (not only by the session-end backstop)',
      firstBeforeStop.length === 1, JSON.stringify(firstBeforeStop));

    const t3Start = t2End + 3000;
    const t3End = await feedTurn(pipe, t3Start, 12, LOST);             // turn 3: lost (closes turn 2)
    await feedTurn(pipe, t3End + 3000, 4, HEARD);                      // turn 4: heard (closes turn 3)
    await sleep(400);
    const beforeStop = lastById(gapsOf(sink.published));
    await pipe.stop();

    const markers = lastById(gapsOf(sink.published)).sort((a, b) => a.start - b.start);
    const spoken = spokenOf(sink.published);
    check('transcribed speech between the losses reached the transcript', spoken.length >= 1);
    check('gap · text · gap = TWO markers (never one merged over transcribed speech)', markers.length === 2,
      JSON.stringify(markers.map((m) => [m.segment_id, m.start, m.end])));
    check('both markers were written before pipe.stop()', beforeStop.length === 2, `${beforeStop.length}`);
    check('marker 1 ends before the transcribed turn starts', !!markers[0] && markers[0].end <= t2Start / 1000 + 0.05,
      `${markers[0]?.end} vs ${t2Start / 1000}`);
    check('marker 2 starts after the transcribed turn ends', !!markers[1] && markers[1].start >= t2End / 1000 - 0.05,
      `${markers[1]?.start} vs ${t2End / 1000}`);
  });
}

async function scenarioC(): Promise<void> {
  console.log('\n[C] a fault the stream recovers from inside the same turn is not a gap');
  await withWarn(async () => {
    const sink = captureSink();
    let down = true;
    let failures = 0;
    const transcribe = async (): Promise<TranscriptionResult> => {
      if (down) { failures++; throw new TranscriptionError('unavailable', 503, undefined, true); }
      return ok();
    };
    const pipe = createBotPipeline(baseInv(), sink, { transcribe, config: FAST });
    await pipe.start();
    // 1.2 s of audio with the provider down (well under the 5 s cap), then it recovers mid-turn.
    await feedTurn(pipe, T0, 18, LOST, (i) => { if (i === 6) down = false; });
    await sleep(300);
    await pipe.stop();
    check('the provider did fail inside the turn (the case is exercised)', failures >= 1, `failures=${failures}`);
    check('the recovered stream published its transcript', spokenOf(sink.published).length >= 1);
    check('NO marker: the buffer kept the audio and the STT heard it on recovery', gapsOf(sink.published).length === 0,
      JSON.stringify(gapsOf(sink.published)));
  });
}

async function scenarioD(): Promise<void> {
  console.log('\n[D] an outage longer than the buffer cap, then recovery → a marker for exactly the discarded span');
  await withWarn(async () => {
    const sink = captureSink();
    let down = true;
    const transcribe = async (): Promise<TranscriptionResult> => {
      if (down) throw new TranscriptionError('unavailable', 503, undefined, true);
      return ok();
    };
    const pipe = createBotPipeline(baseInv(), sink, { transcribe, config: FAST });
    await pipe.start();
    // 8 s of audio with the provider down: the 5 s cap (trySubmit → fullReset) discards the head
    // mid-outage. Then the provider recovers while the same turn keeps speaking.
    await feedTurn(pipe, T0, 52, LOST, (i) => { if (i === 40) down = false; });
    await sleep(300);
    await pipe.stop();

    const markers = lastById(gapsOf(sink.published));
    const m = markers[0];
    const firstSpoken = spokenOf(sink.published).sort((a, b) => a.start - b.start)[0];
    check('the recovered stream published its transcript', !!firstSpoken);
    check('exactly ONE marker (the discarded head is not silently forgotten on recovery)', markers.length === 1,
      JSON.stringify(markers.map((x) => [x.start, x.end])));
    check('the marker starts where the outage started', !!m && Math.abs(m.start - T0 / 1000) < 0.3, `${m?.start} vs ${T0 / 1000}`);
    check('the marker covers the discarded head (~5 s), not the whole 8 s outage the STT re-heard',
      !!m && m.end - m.start >= 4.6 && m.end - m.start <= 6.0, `${m ? (m.end - m.start).toFixed(2) : '-'}s`);
    check('the marker ends where the transcript resumes (no overlap with transcribed audio)',
      !!m && !!firstSpoken && m.end <= firstSpoken.start + 0.05, `${m?.end} vs ${firstSpoken?.start}`);
  });
}

async function scenarioE(): Promise<void> {
  console.log('\n[E] a 3 s outage the buffer still holds, then recovery → no marker');
  await withWarn(async () => {
    const sink = captureSink();
    let down = true;
    let failures = 0;
    const transcribe = async (): Promise<TranscriptionResult> => {
      if (down) { failures++; throw new TranscriptionError('unavailable', 503, undefined, true); }
      return ok();
    };
    const pipe = createBotPipeline(baseInv(), sink, { transcribe, config: FAST });
    await pipe.start();
    await feedTurn(pipe, T0, 27, LOST, (i) => { if (i === 15) down = false; });
    await sleep(300);
    await pipe.stop();
    const firstSpoken = spokenOf(sink.published).sort((a, b) => a.start - b.start)[0];
    check('the provider did fail for the first 3 s (the case is exercised)', failures >= 1, `failures=${failures}`);
    check('the transcript resumes from the outage start (the STT re-heard it)',
      !!firstSpoken && Math.abs(firstSpoken.start - T0 / 1000) < 0.3, `${firstSpoken?.start} vs ${T0 / 1000}`);
    check('NO marker: nothing was lost', gapsOf(sink.published).length === 0, JSON.stringify(gapsOf(sink.published)));
  });
}

async function scenarioF(): Promise<void> {
  console.log('\n[F] a malformed or sub-second span never becomes a marker (and never throws into the lane)');
  await withWarn(async (warned) => {
    const sink = captureSink();
    const fault = new TranscriptionError('unavailable', 503, undefined, true);
    let threw: unknown = null;
    const pipe = createBotPipeline(baseInv({ platform: 'zoom' }), sink, {
      createMixedTranscriber: async (cb) => {
        try {
          cb.onError?.(fault, { startMs: Number.NaN, endMs: Number.NaN });   // a broken clock
          cb.onError?.(fault, { startMs: T0, endMs: T0 + 500 });             // turn-gating noise
        } catch (e) { threw = e; }
        return { feedAudio() {}, recordHint() {}, async dispose() {} };
      },
    });
    await pipe.start();
    await pipe.stop();
    check('a non-finite span does not throw into the lane', threw === null, String(threw));
    check('neither a non-finite nor a sub-second span is marked', gapsOf(sink.published).length === 0,
      JSON.stringify(sink.published));
    check('… nor logged as a gap', !warned.some((w) => /gap/i.test(w)), JSON.stringify(warned));
  });
}

async function main(): Promise<void> {
  await scenarioF();
  await scenarioA();
  await scenarioA2();
  await scenarioB();
  await scenarioC();
  await scenarioD();
  await scenarioE();
  if (failed) { console.error(`\n❌ gap-marker: ${failed} check(s) FAILED.`); process.exit(1); }
  console.log('\n✅ gap-marker (L-0270): lost audio is marked once, when and only where it was lost, content-free; recovered audio never is.');
}

main().catch((e) => { console.error(e); process.exit(1); });
