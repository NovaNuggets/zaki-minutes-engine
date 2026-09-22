/**
 * @vexa/record-chunker — the shared browser MediaRecorder driver.
 *
 * Runs in browser context. Wraps a MediaRecorder over a combined audio
 * MediaStream, encodes each timeslice to base64, and hands it to an injected
 * `onChunk` callback (the recording.v1 chunk shape). On stop it emits one final
 * chunk with `isFinal: true`. NO master assembly here — the master is built
 * server-side (meeting-api `recording_finalizer.py`) from the chunk_seq sequence.
 *
 * Both lane recording taps use this once: `@vexa/gmeet-capture` (gmeet) and
 * `@vexa/mixed-capture-core` (mixed/teams). The combine-the-audio step differs
 * per lane (gmeet builds a combined stream from its media elements; mixed
 * already has one mixed stream) — that lives in each lane; the MediaRecorder
 * loop is identical and lives here.
 *
 * No fallbacks: if `onChunk` throws or returns false we splice the chunk anyway
 * and log (the server-side reconciler re-fetches via the chunk_seq contract);
 * if no supported mimeType exists we log and refuse to start.
 */

/** One recording chunk, ready for upload. Mirrors the recording.v1 wire shape. */
export interface RecordingChunk {
  base64: string;
  chunkSeq: number;
  isFinal: boolean;
  mimeType: string;
}

/** A lane recording tap — what `createGmeetRecordingTap` / `createMixedRecordingTap` return. */
export interface RecordingTap {
  start(): Promise<void>;
  stop(): Promise<void>;
}

/** Options a host passes to a lane recording tap. */
export interface RecordingTapOptions {
  /** MediaRecorder timeslice in ms (default 15000 — matches the WAV chunk size). */
  timesliceMs?: number;
  /** Receives each chunk. Return false / throw → the chunk is spliced anyway (reconciler re-fetches). */
  onChunk: (chunk: RecordingChunk) => Promise<boolean> | boolean;
  /** Fired ONCE on MediaRecorder.onstart — t=0 of the master (segment↔audio alignment). */
  onStarted?: () => void;
}

export interface MediaRecorderChunkerOptions extends RecordingTapOptions {
  /** Combined audio stream to record (lane-built). */
  stream: MediaStream;
}

// Four 8 MiB server-sized chunks is the absolute browser-side working-set ceiling. Normal 15s
// Opus chunks are far smaller and the awaited page binding drains one at a time.
export const RECORDING_PENDING_CHUNK_CAP = 4;
export const RECORDING_CHUNK_MAX_BYTES = 8 * 1024 * 1024;

/** The 4-byte EBML magic every valid webm/Matroska stream starts with (`1a 45 df a3`). */
const EBML_MAGIC = [0x1a, 0x45, 0xdf, 0xa3];

/** True when `bytes` begins with the EBML header (i.e. it is a self-describing webm init segment,
 *  not a cluster-only continuation chunk). */
function isWebmHeader(bytes: Uint8Array): boolean {
  return bytes.length >= 4 && EBML_MAGIC.every((b, i) => bytes[i] === b);
}

const blog = (msg: string) => { try { (window as any).logBot?.(msg); } catch { /* */ } };

/**
 * Drives a MediaRecorder over `stream`, emitting base64 chunks via `onChunk`.
 * Lifecycle: start() → chunks per timeslice → stop() resolves AFTER the final
 * chunk callback completes.
 */
export class MediaRecorderChunker implements RecordingTap {
  private opts: MediaRecorderChunkerOptions;
  private recorder: MediaRecorder | null = null;
  private chunkSeq = 0;
  private pending: Array<{ blob: Blob; seq: number }> = [];
  private drainPromise: Promise<void> | null = null;
  private deliveryFailed = false;
  private stopRequested = false;
  private resolveFinalChunk: (() => void) | null = null;
  private mimeType = "audio/webm";
  /**
   * The webm EBML init segment retained from the FIRST self-describing blob (chunk 0:
   * `1a 45 df a3` EBML + Segment + Tracks + first Cluster). Held so it can be re-attached to a
   * later surviving chunk when chunk 0's own delivery fails over the page→Node base64 bridge;
   * without it the assembler would build a headerless (mid-Matroska `43 b6 75 …`) master from the
   * cluster-only survivors, which no player accepts. */
  private initSegment: Uint8Array | null = null;
  /** False until a chunk carrying the EBML header has been ACK'd by `onChunk` (returned truthy). */
  private initSegmentDelivered = false;

  constructor(opts: MediaRecorderChunkerOptions) {
    this.opts = opts;
  }

  /** The underlying MediaRecorder (null until start()). */
  getMediaRecorder(): MediaRecorder | null {
    return this.recorder;
  }

  async start(): Promise<void> {
    if (this.recorder) {
      blog("[record-chunker] start() called twice — ignoring");
      return;
    }

    // Pick the best supported mimeType. No fallback beyond the candidate list.
    const candidates = ["audio/webm;codecs=opus", "audio/webm", "audio/ogg;codecs=opus", "audio/ogg"];
    let chosen = "";
    for (const mime of candidates) {
      try {
        if ((window as any).MediaRecorder?.isTypeSupported?.(mime)) { chosen = mime; break; }
      } catch { /* */ }
    }

    let recorder: MediaRecorder;
    try {
      recorder = chosen
        ? new MediaRecorder(this.opts.stream, { mimeType: chosen })
        : new MediaRecorder(this.opts.stream);
    } catch (err: any) {
      blog(`[record-chunker] Failed to construct MediaRecorder: ${err?.message || err}`);
      return;
    }

    this.recorder = recorder;
    this.mimeType = recorder.mimeType || chosen || "audio/webm";

    // t=0 of the master — listeners align segment timestamps to audio origin.
    recorder.onstart = () => { try { this.opts.onStarted?.(); } catch { /* */ } };

    recorder.ondataavailable = (event: BlobEvent) => {
      if (!(event.data && event.data.size > 0)) {
        blog("[record-chunker] dataavailable fired with empty data (skipping)");
        return;
      }
      const seq = this.chunkSeq;
      this.chunkSeq = seq + 1;
      if (this.deliveryFailed) return;
      if (event.data.size > RECORDING_CHUNK_MAX_BYTES) {
        this.failDelivery(`chunk ${seq} exceeds ${RECORDING_CHUNK_MAX_BYTES} bytes`);
        return;
      }
      if (this.pending.length >= RECORDING_PENDING_CHUNK_CAP) {
        this.failDelivery(`backpressure queue exceeded ${RECORDING_PENDING_CHUNK_CAP} chunks`);
        return;
      }
      this.pending.push({ blob: event.data, seq });
      if (this.pending.length >= RECORDING_PENDING_CHUNK_CAP) this.pauseRecorder();
      this.scheduleDrain();
    };

    recorder.onstop = () => {
      void this.finishAfterDrain();
    };

    recorder.start(this.opts.timesliceMs ?? 15000);
    blog(`[record-chunker] MediaRecorder started (${this.mimeType}, timeslice=${this.opts.timesliceMs ?? 15000}ms)`);
  }

  async stop(): Promise<void> {
    if (!this.recorder) { blog("[record-chunker] stop() before start() — ignoring"); return; }
    if (this.recorder.state === "inactive") { blog("[record-chunker] recorder already inactive"); return; }

    this.stopRequested = true;
    const finalChunkPromise = new Promise<void>((resolve) => {
      this.resolveFinalChunk = resolve;
      setTimeout(() => {
        if (this.resolveFinalChunk) {
          blog("[record-chunker] final chunk timeout — resolving");
          this.resolveFinalChunk(); this.resolveFinalChunk = null;
        }
      }, 10000);
    });

    try { this.recorder.stop(); }
    catch (err: any) { blog(`[record-chunker] recorder.stop() threw: ${err?.message || err}`); }

    await finalChunkPromise;
  }

  private pauseRecorder(): void {
    if (this.recorder?.state !== "recording") return;
    try { this.recorder.pause(); }
    catch (err: any) { blog(`[record-chunker] recorder.pause failed: ${err?.message || err}`); }
  }

  private resumeRecorder(): void {
    if (this.stopRequested || this.deliveryFailed || this.recorder?.state !== "paused") return;
    try { this.recorder.resume(); }
    catch (err: any) { blog(`[record-chunker] recorder.resume failed: ${err?.message || err}`); }
  }

  private failDelivery(reason: string): void {
    if (this.deliveryFailed) return;
    this.deliveryFailed = true;
    this.pauseRecorder();
    this.pending = [];
    blog(`[record-chunker] recording delivery FAILED: ${reason}`);
  }

  private scheduleDrain(): void {
    if (this.drainPromise || this.deliveryFailed) return;
    this.drainPromise = this.drainPending().finally(() => {
      this.drainPromise = null;
      if (this.pending.length && !this.deliveryFailed) this.scheduleDrain();
    });
  }

  private async waitForDrain(): Promise<void> {
    while (this.drainPromise) await this.drainPromise;
  }

  private async drainPending(): Promise<void> {
    while (this.pending.length && !this.deliveryFailed) {
      const item = this.pending[0];
      try {
        const arrBuffer = await item.blob.arrayBuffer();
        let bytes = new Uint8Array(arrBuffer);
        if (!this.initSegment && isWebmHeader(bytes)) this.initSegment = bytes;
        if (this.initSegment && !this.initSegmentDelivered && !isWebmHeader(bytes)) {
          const merged = new Uint8Array(this.initSegment.length + bytes.length);
          merged.set(this.initSegment, 0);
          merged.set(bytes, this.initSegment.length);
          bytes = merged;
          blog(`[record-chunker] chunk ${item.seq} re-attached EBML init segment (${this.initSegment.length}B)`);
        }
        const carriesHeader = isWebmHeader(bytes);
        let binary = "";
        for (let i = 0; i < bytes.length; i += 0x8000) {
          binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
        }
        const ok = await this.opts.onChunk({
          base64: btoa(binary), chunkSeq: item.seq, isFinal: false, mimeType: this.mimeType,
        });
        if (!ok) throw new Error("sink rejected the chunk");
        if (carriesHeader) this.initSegmentDelivered = true;
        blog(`[record-chunker] chunk ${item.seq} durably acknowledged (${bytes.length} bytes)`);
      } catch (err: any) {
        this.failDelivery(`chunk ${item.seq}: ${err?.message || err}`);
        return;
      }
      this.pending.shift();
      if (this.pending.length < RECORDING_PENDING_CHUNK_CAP) this.resumeRecorder();
    }
  }

  private async finishAfterDrain(): Promise<void> {
    try {
      await this.waitForDrain();
      if (!this.deliveryFailed) {
        const finalSeq = this.chunkSeq;
        this.chunkSeq = finalSeq + 1;
        const ok = await this.opts.onChunk({
          base64: "", chunkSeq: finalSeq, isFinal: true, mimeType: this.mimeType,
        });
        if (!ok) this.failDelivery(`final chunk ${finalSeq} was rejected`);
        else blog(`[record-chunker] final chunk durably acknowledged (seq=${finalSeq})`);
      }
    } catch (err: any) {
      this.failDelivery(`final chunk: ${err?.message || err}`);
    } finally {
      if (this.resolveFinalChunk) { this.resolveFinalChunk(); this.resolveFinalChunk = null; }
    }
  }
}

// ───────────────────────────────────────────────────────────────────────
// createRecordingTap — the full browser recording tap (generic, all platforms)
// ───────────────────────────────────────────────────────────────────────

/**
 * Find active media elements that expose audio. Two-pass (strict → relaxed):
 * tiles can be paused or expose audio via captureStream() rather than a direct
 * srcObject, so the relaxed pass mirrors buildCombinedStream's fallbacks. This
 * is platform-agnostic — a recording grabs every audio element on the page.
 */
async function findMediaElements(retries = 5, delay = 2000): Promise<HTMLMediaElement[]> {
  for (let i = 0; i < retries; i++) {
    const all = Array.from(document.querySelectorAll("audio, video"));
    let els = all.filter((el: any) =>
      !el.paused && el.srcObject instanceof MediaStream && el.srcObject.getAudioTracks().length > 0
    ) as HTMLMediaElement[];
    if (els.length > 0) { blog(`[record-chunker] ${els.length} media elements (strict)`); return els; }

    els = all.filter((el: any) => {
      try {
        if (el.srcObject instanceof MediaStream && el.srcObject.getAudioTracks().length > 0) return true;
        if (typeof el.captureStream === "function" && el.captureStream()?.getAudioTracks?.().length > 0) return true;
        if (typeof el.mozCaptureStream === "function" && el.mozCaptureStream()?.getAudioTracks?.().length > 0) return true;
      } catch { /* not probeable; skip */ }
      return false;
    }) as HTMLMediaElement[];
    if (els.length > 0) { blog(`[record-chunker] ${els.length} media elements (relaxed)`); return els; }

    await new Promise((r) => setTimeout(r, delay));
  }
  return [];
}

/** Mix every media element's audio into one MediaStream via a destination node. */
async function buildCombinedStream(mediaElements: HTMLMediaElement[]): Promise<MediaStream> {
  if (mediaElements.length === 0) throw new Error("[record-chunker] no media elements to combine");
  const ctx = new AudioContext();
  const dest = ctx.createMediaStreamDestination();
  let connected = 0;
  mediaElements.forEach((element: any, index) => {
    try {
      const s =
        element.srcObject ||
        (element.captureStream && element.captureStream()) ||
        (element.mozCaptureStream && element.mozCaptureStream());
      if (s instanceof MediaStream && s.getAudioTracks().length > 0) {
        ctx.createMediaStreamSource(s).connect(dest);
        connected++;
        blog(`[record-chunker] connected element ${index + 1}/${mediaElements.length}`);
      }
    } catch (e: any) { blog(`[record-chunker] could not connect element ${index + 1}: ${e?.message || e}`); }
  });
  if (connected === 0) throw new Error("[record-chunker] could not connect any audio streams");
  blog(`[record-chunker] combined ${connected} streams`);
  return dest.stream;
}

/** Options for createRecordingTap — combine all audio elements then optionally override. */
export interface CreateRecordingTapOptions extends RecordingTapOptions {
  /** Provide a ready stream to record (e.g. the mixed-lane tab stream); else all audio elements are combined. */
  stream?: MediaStream;
}

/**
 * The browser recording tap, used by BOTH lanes (gmeet, teams) and both hosts
 * (bot, extension): find every audio element → combine → `MediaRecorderChunker`
 * → recording.v1 chunks via `onChunk`. Recording is platform-agnostic — it
 * records the whole meeting mix — so this is ONE generic tap, not per-lane.
 * (Zoom records via node PulseAudio in @vexa/recording, no browser tap.)
 *
 * Pass `opts.stream` to record a ready stream directly (skips the element
 * combine); otherwise it finds + combines the page's audio elements.
 */
export function createRecordingTap(opts: CreateRecordingTapOptions): RecordingTap {
  let chunker: MediaRecorderChunker | null = null;
  return {
    async start(): Promise<void> {
      let stream = opts.stream;
      if (!stream) {
        const els = await findMediaElements();
        if (els.length === 0) { blog("[record-chunker] no media elements — cannot record"); return; }
        stream = await buildCombinedStream(els);
      }
      chunker = new MediaRecorderChunker({
        stream,
        timesliceMs: opts.timesliceMs ?? 15000,
        onChunk: opts.onChunk,
        onStarted: opts.onStarted,
      });
      await chunker.start();
    },
    async stop(): Promise<void> {
      await chunker?.stop();
      chunker = null;
    },
  };
}
