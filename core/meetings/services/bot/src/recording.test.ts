/**
 * L3 — recording sink (recording.v1 accumulate → assemble master). OFFLINE, NO disk/HTTP.
 *
 * Drives the REAL @vexa/recording assembler through the bot's `createBotRecordingSink` with an
 * injected `onMaster` (so we assert the assembled master WITHOUT a meeting-api receiver), and
 * asserts:
 *   • chunks accumulate per key; the empty is_final chunk assembles + emits the master;
 *   • close(key) is the ROBUST trigger — it assembles even when the trailing is_final chunk
 *     never arrives (the live Stop race that loses the last MediaRecorder chunk);
 *   • the assembled master is the byte-concat / RIFF-merge the recording.v1 codec defines
 *     (webm byte-concat; wav header-stripped PCM merge), with the right key/format/chunk count;
 *   • out-of-order seqs are sorted at assembly.
 * Run: npx tsx src/recording.test.ts
 */
import { createBotRecordingSink } from './recording.js';
import type { Invocation } from './config.js';
import type { RecordingMaster } from '@vexa/recording';

let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond ? '' : '  — ' + detail}`);
  if (!cond) failed++;
};

const inv = (over: Partial<Invocation> = {}): Invocation => ({
  platform: 'google_meet', meetingUrl: 'https://meet.google.com/abc-defg-hij', botName: 'Vexa',
  redisUrl: 'redis://localhost:6379', recordingEnabled: true, ...over,
});

/** Build a canonical 44-byte-header WAV chunk wrapping `data` (mirrors the recording codec's
 *  expected per-chunk RIFF shape — fmt: PCM, mono, 16kHz, 16bps). */
const FMT = Buffer.from([0x01, 0x00, 0x01, 0x00, 0x80, 0x3e, 0x00, 0x00, 0x00, 0x7d, 0x00, 0x00, 0x02, 0x00, 0x10, 0x00]);
function wavChunk(data: Buffer): Buffer {
  const h = Buffer.alloc(44);
  h.write('RIFF', 0, 'ascii'); h.writeUInt32LE(36 + data.length, 4); h.write('WAVE', 8, 'ascii');
  h.write('fmt ', 12, 'ascii'); h.writeUInt32LE(16, 16); FMT.copy(h, 20);
  h.write('data', 36, 'ascii'); h.writeUInt32LE(data.length, 40);
  return Buffer.concat([h, data]);
}

async function main(): Promise<void> {
  // ── 1) webm: accumulate chunks, empty is_final assembles the master (byte-concat) ──
  {
    const masters: RecordingMaster[] = [];
    const sink = createBotRecordingSink({ inv: inv(), onMaster: (m) => masters.push(m) });
    const c0 = Buffer.from([0x1a, 0x45, 0xdf, 0xa3]); // self-describing chunk 0
    const c1 = Buffer.from([0xa3, 0x01, 0x02]);       // cluster-only
    await sink.chunk('google_meet/m1', 0, false, 'webm', c0);
    await sink.chunk('google_meet/m1', 1, false, 'webm', c1);
    await sink.chunk('google_meet/m1', 2, true, 'webm', new Uint8Array(0)); // empty final = COMPLETED signal

    check('is_final: exactly one master emitted', masters.length === 1, String(masters.length));
    check('is_final: master keyed by the session key', masters[0]?.key === 'google_meet/m1', masters[0]?.key);
    check('is_final: master format = webm', masters[0]?.format === 'webm', masters[0]?.format);
    check('is_final: chunk count = 2 non-empty', masters[0]?.chunks === 2, String(masters[0]?.chunks));
    check('is_final: webm master == byte-concat(c0,c1)', !!masters[0] && Buffer.from(masters[0].bytes).equals(Buffer.concat([c0, c1])),
      masters[0] ? Buffer.from(masters[0].bytes).toString('hex') : 'no master');
  }

  // ── 2) close(key) assembles even without an is_final chunk (the robust trigger) ──
  {
    const masters: RecordingMaster[] = [];
    const sink = createBotRecordingSink({ inv: inv(), onMaster: (m) => masters.push(m) });
    const a = Buffer.from([0x11, 0x22, 0x33, 0x44]);
    const b = Buffer.from([0x55, 0x66]);
    await sink.chunk('google_meet/m2', 0, false, 'wav', wavChunk(a));
    await sink.chunk('google_meet/m2', 1, false, 'wav', wavChunk(b));
    check('close: nothing assembled before close', masters.length === 0, String(masters.length));
    await sink.close('google_meet/m2');   // host-side close (the WS dropped) — robust assembly trigger
    check('close: master assembled on close (no is_final needed)', masters.length === 1, String(masters.length));
    check('close: wav master payload == concat of stripped PCM (a,b)',
      !!masters[0] && Buffer.from(masters[0].bytes).subarray(44).equals(Buffer.concat([a, b])),
      masters[0] ? Buffer.from(masters[0].bytes).subarray(44).toString('hex') : 'no master');
    check('close: wav master chunk count = 2', masters[0]?.chunks === 2, String(masters[0]?.chunks));
  }

  // ── 3) out-of-order seqs are sorted at assembly ──
  {
    const masters: RecordingMaster[] = [];
    const sink = createBotRecordingSink({ inv: inv(), onMaster: (m) => masters.push(m) });
    const c0 = Buffer.from([0xaa]);
    const c1 = Buffer.from([0xbb]);
    const c2 = Buffer.from([0xcc]);
    await sink.chunk('google_meet/m3', 2, false, 'webm', c2);   // arrive out of order
    await sink.chunk('google_meet/m3', 0, false, 'webm', c0);
    await sink.chunk('google_meet/m3', 1, false, 'webm', c1);
    await sink.close('google_meet/m3');
    check('order: webm master == byte-concat in SEQ order (c0,c1,c2)',
      !!masters[0] && Buffer.from(masters[0].bytes).equals(Buffer.concat([c0, c1, c2])),
      masters[0] ? Buffer.from(masters[0].bytes).toString('hex') : 'no master');
  }

  // ── 4) close on an empty/never-fed session is a no-op (no spurious master) ──
  {
    const masters: RecordingMaster[] = [];
    const sink = createBotRecordingSink({ inv: inv(), onMaster: (m) => masters.push(m) });
    await sink.close('google_meet/never');
    check('empty session: close is a no-op (no master)', masters.length === 0, String(masters.length));
  }

  // ── 5) default upload path delegates only the per-spawn MeetingToken ──
  {
    const uploads: Array<{ url: string; token: string; meetingId: number | string; sessionUid: string; seq: number; final: boolean }> = [];
    const scoped = inv({
      contractVersion: 'invocation.v2',
      meeting_id: 42,
      connectionId: 'session-42',
      token: 'spawn-scoped-meeting-token',
      internalSecret: 'legacy-platform-secret-must-be-ignored',
      recordingUploadUrl: 'http://meeting-api:8080/internal/recordings/upload',
    });
    const sink = createBotRecordingSink({
      inv: scoped,
      recordingChunkServiceFactory: (meetingId, sessionUid) => ({
        uploadChunk: async (url, token, _bytes, seq, final) => {
          uploads.push({ url, token, meetingId, sessionUid, seq, final });
        },
      }),
    });
    await sink.chunk('google_meet/upload', 0, false, 'webm', Buffer.from([0x01]));
    await sink.close('google_meet/upload');

    check('upload auth: media + synthesized final are uploaded', uploads.length === 2, JSON.stringify(uploads));
    check('upload auth: MeetingToken bearer input is delegated', uploads[0]?.token === 'spawn-scoped-meeting-token', JSON.stringify(uploads[0]));
    check('upload auth: platform internal secret is ignored', uploads[0]?.token !== scoped.internalSecret, JSON.stringify(uploads[0]));
    check('upload auth: token remains session-bound', uploads[0]?.meetingId === 42 && uploads[0]?.sessionUid === 'session-42', JSON.stringify(uploads[0]));
    check('upload order: close synthesizes the contiguous final signal', uploads[0]?.seq === 0 && !uploads[0]?.final && uploads[1]?.seq === 1 && uploads[1]?.final, JSON.stringify(uploads));
  }

  // ── 6) managed capture durably ACKs every chunk and remains bounded for 90 minutes ──
  {
    const uploaded: number[] = [];
    let active = 0;
    let peakActive = 0;
    const sink = createBotRecordingSink({
      inv: inv({
        contractVersion: 'invocation.v2', meeting_id: 91, connectionId: 'managed-91',
        token: 'meeting-token', recordingUploadUrl: 'http://meeting-api:8080/internal/recordings/upload',
      }),
      recordingChunkServiceFactory: () => ({
        uploadChunk: async (_url, _token, _bytes, seq) => {
          active++;
          peakActive = Math.max(peakActive, active);
          await Promise.resolve();
          uploaded.push(seq);
          active--;
        },
      }),
    });
    // One 15-second chunk for a 90-minute meeting. Awaiting each ACK models the page binding's
    // backpressure: no master or meeting-long byte array is retained in the worker.
    for (let seq = 0; seq < 360; seq++) {
      await sink.chunk('google_meet/managed-91', seq, false, 'webm', Buffer.alloc(1024));
    }
    await sink.close('google_meet/managed-91');
    check('90-minute stream: all 360 media chunks + final are durably uploaded', uploaded.length === 361, String(uploaded.length));
    check('90-minute stream: transport is strictly ordered', uploaded.every((seq, i) => seq === i), JSON.stringify(uploaded.slice(-5)));
    check('90-minute stream: at most one chunk is in transport memory', peakActive === 1, String(peakActive));
  }

  // ── 7) graceful completion awaits the durable final signal ──
  {
    let releaseFinal: (() => void) | undefined;
    let finalStarted = false;
    let finalSettled = false;
    const sink = createBotRecordingSink({
      inv: inv({ contractVersion: 'invocation.v2', meeting_id: 92, connectionId: 'managed-92', token: 'token', recordingUploadUrl: 'http://meeting-api/upload' }),
      recordingChunkServiceFactory: () => ({
        uploadChunk: async (_url, _token, _bytes, _seq, final) => {
          if (!final) return;
          finalStarted = true;
          await new Promise<void>((resolve) => { releaseFinal = resolve; });
          finalSettled = true;
        },
      }),
    });
    await sink.chunk('google_meet/managed-92', 0, false, 'webm', Buffer.from([1]));
    const close = sink.close('google_meet/managed-92');
    await new Promise<void>((resolve) => setImmediate(resolve));
    check('graceful await: final upload started', finalStarted);
    check('graceful await: close is still pending while final upload is pending', !finalSettled);
    releaseFinal?.();
    await close;
    check('graceful await: close resolves only after final durability ACK', finalSettled);
  }

  // ── 8) permanent upload failures are surfaced and prevent a false final ──
  {
    let calls = 0;
    const sink = createBotRecordingSink({
      inv: inv({ contractVersion: 'invocation.v2', meeting_id: 93, connectionId: 'managed-93', token: 'token', recordingUploadUrl: 'http://meeting-api/upload' }),
      recordingChunkServiceFactory: () => ({
        uploadChunk: async () => { calls++; throw new Error('permanent transport failure'); },
      }),
    });
    let chunkFailed = false;
    try { await sink.chunk('google_meet/managed-93', 0, false, 'webm', Buffer.from([1])); }
    catch { chunkFailed = true; }
    let closeFailed = false;
    try { await sink.close('google_meet/managed-93'); }
    catch { closeFailed = true; }
    check('failed upload: chunk ACK rejects', chunkFailed);
    check('failed upload: close surfaces the latched failure', closeFailed);
    check('failed upload: no later final is attempted across the gap', calls === 1, String(calls));
  }

  // ── 9) retention revoke aborts in-flight IO and blocks queued/post-fence sends ──
  {
    let sends = 0;
    let sawAbort = false;
    const sink = createBotRecordingSink({
      inv: inv({ contractVersion: 'invocation.v2', meeting_id: 94, connectionId: 'managed-94', token: 'token', recordingUploadUrl: 'http://meeting-api/upload' }),
      recordingChunkServiceFactory: () => ({
        uploadChunk: async (_url, _token, _bytes, _seq, _final, _format, signal) => {
          sends++;
          await new Promise<void>((resolve, reject) => {
            signal?.addEventListener('abort', () => { sawAbort = true; reject(new Error('aborted')); }, { once: true });
          });
        },
      }),
    });
    const inFlight = sink.chunk('google_meet/managed-94', 0, false, 'webm', Buffer.from([1])).catch(() => {});
    await new Promise<void>((resolve) => setImmediate(resolve));
    sink.revoke();
    await inFlight;
    await sink.chunk('google_meet/managed-94', 1, false, 'webm', Buffer.from([2]));
    await sink.close('google_meet/managed-94');
    check('retention race: the active request is aborted', sawAbort);
    check('retention race: queued/final/post-fence sends are suppressed', sends === 1, String(sends));
  }

  if (failed) { console.error(`\n❌ recording (L3): ${failed} check(s) FAILED.`); process.exit(1); }
  console.log('\n✅ recording (L3): recording.v1 chunks accumulate → buildRecordingMaster on is_final OR close; webm byte-concat + wav RIFF-merge, seq-ordered (real assembler · injected onMaster · no disk/HTTP).');
}

void main();
