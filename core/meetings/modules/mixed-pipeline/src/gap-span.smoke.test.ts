/**
 * gap-span.smoke (L-0270) — a turn whose audio was LOST to a dead STT must report
 * the lost SPAN, not just the fault.
 *
 * The incident: the provider 503'd for 14 minutes; the client retried 4×, threw, the
 * chunk was dropped, and the transcript the reader got had no hole in it — it simply
 * skipped the missing minutes. `onError(fault)` alone cannot be turned into a gap
 * marker downstream because it carries no time. So: when a turn CLOSES having
 * confirmed nothing and having faulted, the lane reports `onError(fault, {startMs,
 * endMs})` — the audio-time span the reader never got.
 *
 * A fault on an OPEN turn is NOT a gap: the window is re-submitted on the next tick,
 * so only the close (nothing confirmed, at least one fault) proves the audio is gone.
 * Run: npx tsx src/gap-span.smoke.test.ts
 */
import { ChunkedTranscriber, type BoundarySource } from './index.js';
import type { BoundaryEvent } from './pyannote-segmenter.js';
import { TranscriptionError } from '@vexa/transcribe-whisper';

const SAMPLE_RATE = 16000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond ? '' : '  — ' + detail}`);
  if (!cond) failed++;
};

type Span = { startMs: number; endMs: number } | undefined;

// ── 1) the outage: every submission throws the provider's 503 ──────────────────
{
  let emit: (ev: BoundaryEvent) => void = () => {};
  const faults: Array<{ fault: unknown; span: Span }> = [];
  const published: Array<{ startMs: number; endMs: number; text: string }> = [];

  const tc = await ChunkedTranscriber.create({
    transcribe: async () => { throw new TranscriptionError('unavailable', 503, 'Service unavailable', true); },
    publish: (_speaker, confirmed) => { published.push(...confirmed); },
    publishPending: () => {},
    clearPending: () => {},
    rename: () => {},
    onError: (fault, span) => { faults.push({ fault, span }); },
    makeSegmenter: async (onBoundary): Promise<BoundarySource> => {
      emit = onBoundary;
      return { appendFrame: async () => {}, reset() {} };
    },
  });

  emit({ tMs: 0, kind: 'silence→speaker', confidence: 0.9 });
  await sleep(25);
  const halfSecond = new Float32Array(SAMPLE_RATE / 2).fill(0.1);
  for (let t = 0; t <= 2000; t += 500) tc.feedAudio(halfSecond, t);
  emit({ tMs: 2000, kind: 'speaker→silence', confidence: 0.9 });
  await sleep(150);
  await tc.dispose();

  const spanned = faults.filter((f) => !!f.span);
  const span = spanned[spanned.length - 1]?.span;
  check('nothing was published (the audio is gone)', published.length === 0, JSON.stringify(published));
  check('the fault itself is still surfaced (P18 unchanged)', faults.length >= 1, `got ${faults.length}`);
  check('the LOST SPAN is reported exactly once for the turn', spanned.length === 1, `got ${spanned.length}`);
  check('the span covers the turn (0..2000ms audio time)',
    !!span && span.startMs <= 1 && span.endMs >= 1900 && span.endMs <= 2100,
    JSON.stringify(span));
  check('the span arrives with the typed fault (attributable reason)',
    spanned[0]?.fault instanceof TranscriptionError &&
      (spanned[0]?.fault as TranscriptionError).status === 503,
    String(spanned[0]?.fault));
}

// ── 2) a healthy turn reports NO span (no phantom gaps) ───────────────────────
{
  let emit: (ev: BoundaryEvent) => void = () => {};
  const faults: Array<{ fault: unknown; span: Span }> = [];
  const published: Array<{ startMs: number; endMs: number; text: string }> = [];

  const tc = await ChunkedTranscriber.create({
    transcribe: async () => ({
      text: 'all good', language: 'en', language_probability: 0.99, duration: 2,
      segments: [{ text: 'all good', start: 0, end: 2.0, no_speech_prob: 0.01, avg_logprob: -0.1, compression_ratio: 1.0 } as any],
    }),
    publish: (_speaker, confirmed) => { published.push(...confirmed); },
    publishPending: () => {},
    clearPending: () => {},
    rename: () => {},
    onError: (fault, span) => { faults.push({ fault, span }); },
    makeSegmenter: async (onBoundary): Promise<BoundarySource> => {
      emit = onBoundary;
      return { appendFrame: async () => {}, reset() {} };
    },
  });

  emit({ tMs: 0, kind: 'silence→speaker', confidence: 0.9 });
  await sleep(25);
  const halfSecond = new Float32Array(SAMPLE_RATE / 2).fill(0.1);
  for (let t = 0; t <= 2000; t += 500) tc.feedAudio(halfSecond, t);
  emit({ tMs: 2000, kind: 'speaker→silence', confidence: 0.9 });
  await sleep(150);
  await tc.dispose();

  check('a transcribed turn publishes text', published.length >= 1, JSON.stringify(published));
  check('a transcribed turn reports NO gap span', faults.filter((f) => !!f.span).length === 0,
    JSON.stringify(faults.map((f) => f.span)));
}

const seg = (text: string, start: number, end: number): any =>
  ({ text, start, end, no_speech_prob: 0.01, avg_logprob: -0.1, compression_ratio: 1.0 });
const said = (segs: any[]) =>
  ({ text: segs.map((s) => s.text).join(' '), language: 'en', language_probability: 0.99, duration: 2, segments: segs });

async function harness(transcribe: () => Promise<any>) {
  let emit: (ev: BoundaryEvent) => void = () => {};
  const faults: Array<{ fault: unknown; span: Span }> = [];
  const published: Array<{ startMs: number; endMs: number; text: string }> = [];
  const tc = await ChunkedTranscriber.create({
    language: 'en',
    transcribe,
    publish: (_speaker, confirmed) => { published.push(...confirmed); },
    publishPending: () => {},
    clearPending: () => {},
    rename: () => {},
    onError: (fault, span) => { faults.push({ fault, span }); },
    log: () => {},
    makeSegmenter: async (onBoundary): Promise<BoundarySource> => {
      emit = onBoundary;
      return { appendFrame: async () => {}, reset() {} };
    },
  });
  const feed = (fromMs: number, toMs: number) => {
    const halfSecond = new Float32Array(SAMPLE_RATE / 2).fill(0.1);
    for (let t = fromMs; t < toMs; t += 500) tc.feedAudio(halfSecond, t);
  };
  return { tc, faults, published, feed, emit: (kind: BoundaryEvent['kind'], tMs: number) => emit({ tMs, kind, confidence: 0.9 }) };
}
const down = () => new TranscriptionError('unavailable', 503, 'Service unavailable', true);

// ── 3) a fault the turn RECOVERS from reports no lost span ─────────────────────
// A tick fails, then the closing pass (same window, from the same confirmed edge) succeeds with
// speech ending at 1.2 s of a 3 s turn: the STT heard all of it, so nothing is lost — not even the
// silent 1.8 s after the last word.
{
  let up = false;
  const h = await harness(async () => {
    if (!up) throw down();
    return said([seg('alpha beta', 0, 1.2)]);
  });
  h.emit('silence→speaker', 0);
  await sleep(25);
  h.feed(0, 3000);
  await sleep(2300);                       // one heartbeat tick submits [0, 3000] — and fails
  const faultsBeforeRecovery = h.faults.length;
  up = true;
  h.emit('speaker→silence', 3000);
  await sleep(200);
  await h.tc.dispose();
  check('[recovered] the tick did fail before the close (the case is exercised)', faultsBeforeRecovery >= 1, `${faultsBeforeRecovery}`);
  check('[recovered] the closing pass published the speech', h.published.some((p) => p.text === 'alpha beta'), JSON.stringify(h.published));
  check('[recovered] a turn that faulted and then was heard reports NO lost span',
    h.faults.filter((f) => !!f.span).length === 0, JSON.stringify(h.faults.map((f) => f.span)));
}

// ── 4) a turn that confirmed speech, THEN lost its tail to the outage ───────────
// Three stable ticks confirm the leading words, the provider dies, the turn closes: the lost span
// is exactly the unpublished tail [confirmed edge, t1] — not the whole turn, and not nothing.
{
  let calls = 0;
  let up = true;
  const h = await harness(async () => {
    if (!up) throw down();
    calls++;
    return said([seg('alpha', 0, 0.8), seg('beta', 0.8, 1.6), seg('gamma', 1.6, 2.4), seg(`tail${calls}`, 2.4, 2.8)]);
  });
  h.emit('silence→speaker', 0);
  await sleep(25);
  let edge = 0;
  for (let pass = 0; pass < 4 && h.published.length === 0; pass++) {
    h.feed(edge, edge + 2500); edge += 2500;
    await sleep(1100);                    // one heartbeat → one tick (≥2 s of new audio each)
  }
  const confirmed = h.published.slice();
  const confirmedEdge = Math.max(0, ...confirmed.map((p) => p.endMs));
  up = false;                             // the provider dies mid-turn
  h.feed(edge, edge + 2500); edge += 2500;
  await sleep(1100);
  h.emit('speaker→silence', edge);
  await sleep(300);
  await h.tc.dispose();
  const spanned = h.faults.filter((f) => !!f.span);
  const span = spanned[0]?.span;
  check('[tail] speech was confirmed before the outage (the case is exercised)', confirmed.length >= 1, JSON.stringify(h.published));
  check('[tail] the lost tail is reported exactly once', spanned.length === 1, JSON.stringify(spanned.map((f) => f.span)));
  check('[tail] the span starts at the confirmed edge — the transcribed head is not claimed',
    !!span && Math.abs(span.startMs - confirmedEdge) < 50, `${span?.startMs} vs ${confirmedEdge}`);
  check('[tail] the span runs to the turn close', !!span && Math.abs(span.endMs - edge) <= 500, `${span?.endMs} vs ${edge}`);
}

if (failed) {
  console.error(`\n❌ gap-span: ${failed} check(s) FAILED.`);
  process.exit(1);
}
console.log('\n✅ gap-span (L-0270): a turn lost to a dead STT reports its span once; a healthy turn reports none.');
