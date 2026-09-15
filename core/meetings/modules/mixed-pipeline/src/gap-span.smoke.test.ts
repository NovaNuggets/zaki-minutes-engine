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

if (failed) {
  console.error(`\n❌ gap-span: ${failed} check(s) FAILED.`);
  process.exit(1);
}
console.log('\n✅ gap-span (L-0270): a turn lost to a dead STT reports its span once; a healthy turn reports none.');
