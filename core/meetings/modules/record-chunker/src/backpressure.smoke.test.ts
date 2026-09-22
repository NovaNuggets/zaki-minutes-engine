/** Managed recording browser backpressure: bounded queue, ordered durable ACKs, awaited final. */
const enc = (s: string): string => Buffer.from(s, 'binary').toString('base64');
(globalThis as any).btoa = (s: string) => enc(s);
(globalThis as any).window = { logBot: (_m: string) => {} };

class FakeBlob {
  constructor(private bytes: Uint8Array) {}
  get size() { return this.bytes.length; }
  async arrayBuffer(): Promise<ArrayBuffer> {
    return this.bytes.buffer.slice(this.bytes.byteOffset, this.bytes.byteOffset + this.bytes.byteLength);
  }
}

class FakeMediaRecorder {
  static isTypeSupported(mime: string) { return mime === 'audio/webm;codecs=opus'; }
  onstart: (() => void) | null = null;
  ondataavailable: ((event: any) => void) | null = null;
  onstop: (() => void) | null = null;
  state: 'inactive' | 'recording' | 'paused' = 'inactive';
  mimeType: string;
  pauses = 0;
  resumes = 0;
  constructor(_stream: any, opts?: { mimeType?: string }) { this.mimeType = opts?.mimeType ?? ''; }
  start() { this.state = 'recording'; this.onstart?.(); }
  pause() { if (this.state === 'recording') { this.state = 'paused'; this.pauses++; } }
  resume() { if (this.state === 'paused') { this.state = 'recording'; this.resumes++; } }
  stop() { this.state = 'inactive'; this.onstop?.(); }
  emit(bytes: Uint8Array) {
    if (this.state === 'recording') this.ondataavailable?.({ data: new FakeBlob(bytes) });
  }
}
(globalThis as any).MediaRecorder = FakeMediaRecorder;
(globalThis as any).window.MediaRecorder = FakeMediaRecorder;

import { MediaRecorderChunker, RECORDING_PENDING_CHUNK_CAP } from './index';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));

async function main(): Promise<void> {
  let releaseFirst: (() => void) | undefined;
  let active = 0;
  let peakActive = 0;
  const seqs: number[] = [];
  const chunker = new MediaRecorderChunker({
    stream: {} as any,
    onChunk: async (chunk) => {
      active++;
      peakActive = Math.max(peakActive, active);
      if (chunk.chunkSeq === 0) await new Promise<void>((resolve) => { releaseFirst = resolve; });
      seqs.push(chunk.chunkSeq);
      active--;
      return true;
    },
  });
  await chunker.start();
  const recorder = chunker.getMediaRecorder() as unknown as FakeMediaRecorder;

  for (let seq = 0; seq < RECORDING_PENDING_CHUNK_CAP; seq++) {
    recorder.emit(new Uint8Array([seq + 1]));
  }
  await tick();
  if (recorder.state !== 'paused' || recorder.pauses !== 1) {
    throw new Error(`recorder did not pause at queue cap: state=${recorder.state}, pauses=${recorder.pauses}`);
  }
  releaseFirst?.();
  await tick();
  await tick();
  if (peakActive !== 1) throw new Error(`chunk deliveries overlapped: peak=${peakActive}`);
  if (recorder.resumes !== 1 || recorder.state !== 'recording') {
    throw new Error(`recorder did not resume after drain: state=${recorder.state}, resumes=${recorder.resumes}`);
  }

  // 90 minutes at one 15-second timeslice. Give the durable callback a turn after each event,
  // exactly as the production Playwright binding does; memory use stays independent of duration.
  for (let seq = RECORDING_PENDING_CHUNK_CAP; seq < 360; seq++) {
    recorder.emit(new Uint8Array([seq % 251]));
    await tick();
  }
  await chunker.stop();
  if (seqs.length !== 361 || seqs.some((seq, index) => seq !== index)) {
    throw new Error(`unexpected durable sequence: count=${seqs.length}, tail=${seqs.slice(-5)}`);
  }
  console.log('✅ backpressure.smoke: 90-minute stream stays ordered/serial, pauses at four queued blobs, resumes after ACK, and awaits final.');
}

main().catch((error) => { console.error('❌ backpressure.smoke:', error); process.exit(1); });
