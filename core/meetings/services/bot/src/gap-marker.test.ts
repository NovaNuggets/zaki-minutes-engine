/**
 * L3 (L-0270) — the TRANSCRIPT GAP MARKER. OFFLINE, NO browser/whisper/redis.
 *
 * The incident (staging meeting 49, 2026-09-15): the STT provider returned HTTP 503
 * for 14 minutes. The client retried 4× per chunk, threw, and the lane dropped the
 * audio. The stored transcript had NO hole in it — the missing minutes were simply
 * absent, the summary read as complete, and nothing told the reader that a quarter of
 * the meeting was never transcribed.
 *
 * This pins the truth leg: audio lost to a dead STT becomes a GAP MARKER segment on
 * the same transcript.v1 egress as any other segment (so it survives reload and reaches
 * the summariser), carrying the covered span and the provider reason — and contiguous
 * losses COALESCE into one marker (the incident would otherwise have produced dozens),
 * logged once at warn with the span instead of one line per failed attempt.
 *
 * Run: npx tsx src/gap-marker.test.ts
 */
import Ajv2020, { type ValidateFunction } from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createBotPipeline } from './pipeline.js';
import type { Invocation } from './config.js';
import type { TranscriptSegment } from './contracts.js';
import type { TranscriptSink } from './ports.js';
import { TranscriptionError } from '@vexa/transcribe-whisper';

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

const baseInv = (over: Partial<Invocation> = {}): Invocation => ({
  platform: 'google_meet', meetingUrl: 'https://meet.google.com/abc-defg-hij', botName: 'Vexa',
  redisUrl: 'redis://localhost:6379', transcribeEnabled: true, ...over,
});

const SR = 16000;
const FRAME_MS = 200;
const FRAME = new Float32Array((SR * FRAME_MS) / 1000).fill(0.05);
const FAST = { minAudioDuration: 0.15, submitInterval: 0.1, confirmThreshold: 2, maxBufferDuration: 5, idleTimeoutSec: 2, sampleRate: SR };
/** The incident's wall clock (09:03:50Z, the first 503 on meeting 49). */
const T0 = Date.UTC(2026, 8, 15, 9, 3, 50);

/** Feed one contiguous channel turn of `frames` frames starting at `startTs`. Returns the next ts. */
async function feedTurn(pipe: { feedAudio: (c: number, g: string | undefined, p: Float32Array, t: number) => void }, startTs: number, frames: number): Promise<number> {
  let ts = startTs;
  for (let i = 0; i < frames; i++) { pipe.feedAudio(0, 'Alice', FRAME, ts); ts += FRAME_MS; await sleep(110); }
  return ts;
}

async function main(): Promise<void> {
  const warned: string[] = [];
  const realWarn = console.warn;
  console.warn = (...a: unknown[]) => { warned.push(a.map(String).join(' ')); };

  const sink = captureSink();
  const transcribe = async (): Promise<never> => {
    throw new TranscriptionError('unavailable', 503, 'Service unavailable', true);
  };
  const pipe = createBotPipeline(baseInv(), sink, { transcribe, config: FAST });
  await pipe.start();

  // Two channel turns (a >1s feed gap rotates the turn), both lost to the 503 —
  // exactly the incident's shape: consecutive dropped windows, no text in between.
  const afterFirst = await feedTurn(pipe, T0, 12);
  await feedTurn(pipe, afterFirst + 3000, 12);
  await sleep(300);
  await pipe.stop();
  console.warn = realWarn;

  const gaps = sink.published.filter((s) => s.segment_id.startsWith('gap:'));
  const last = gaps[gaps.length - 1];
  const ids = new Set(gaps.map((s) => s.segment_id));
  const spoken = sink.published.filter((s) => !s.segment_id.startsWith('gap:') && (s.text ?? '').trim());

  check('the dropped audio produced a GAP MARKER segment', gaps.length >= 1, JSON.stringify(sink.published));
  check('no phantom transcript text was published', spoken.length === 0, JSON.stringify(spoken));
  check('contiguous losses COALESCE into ONE marker (not one per dropped window)',
    ids.size === 1, `${ids.size} ids: ${[...ids].join(', ')}`);
  check('the marker is a completed segment (survives reload like any other)',
    last?.completed === true, JSON.stringify(last));
  check('the marker is speaker-neutral (no person is credited with the silence)',
    last?.speaker === 'system', last?.speaker);
  check('the marker text names the covered span and the provider reason',
    !!last && /transcription unavailable/i.test(last.text) && last.text.includes('09:03:50') && last.text.includes('503'),
    last?.text);
  check('the marker SPAN starts at the lost audio (epoch seconds)',
    !!last && Math.abs(last.start - T0 / 1000) < 1.5, `${last?.start} vs ${T0 / 1000}`);
  check('the marker SPAN was EXTENDED over the whole outage (both turns)',
    !!last && last.end - last.start >= 7, `${(last ? last.end - last.start : 0).toFixed(1)}s`);
  check('the marker carries absolute times (the reader sees when)',
    !!last?.absolute_start_time && !!last?.absolute_end_time &&
      Math.abs(new Date(last.absolute_start_time).getTime() / 1000 - last.start) < 1,
    `${last?.absolute_start_time}..${last?.absolute_end_time}`);
  check('every published segment is transcript.v1-valid (ajv vs SSOT)',
    sink.published.length > 0 && sink.published.every((s) => !!validateSeg(s)), ajv.errorsText(validateSeg.errors));
  check('the gap is logged ONCE at warn with the span (not one line per attempt)',
    warned.filter((w) => /gap/i.test(w)).length === 1, JSON.stringify(warned));

  if (failed) { console.error(`\n❌ gap-marker: ${failed} check(s) FAILED.`); process.exit(1); }
  console.log('\n✅ gap-marker (L-0270): audio lost to a dead STT reaches the transcript as one coalesced, spanned, speaker-neutral marker.');
}

main().catch((e) => { console.error(e); process.exit(1); });
