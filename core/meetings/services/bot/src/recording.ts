/**
 * RecordingSink adapter (2b) — the recording.v1 finalize + upload path, behind the
 * orchestrator's RecordingSink port (`close(key)`).
 *
 * recording.v1 has TWO halves (both in @vexa/recording): ACQUIRE (the browser MediaRecorder
 * tap → chunks) and DELIVER (chunk-upload over HTTP to meeting-api). The ACQUIRE side is
 * browser-resident and L4-gated (it lives in capture-bridge.ts); THIS file is the DELIVER +
 * finalize core:
 *   - chunk(seq, isFinal, format, bytes) accumulates recording.v1 chunks (createRecordingAssembler);
 *   - the empty is_final chunk OR close(key) triggers buildRecordingMaster → onMaster;
 *   - onMaster uploads the assembled master to inv.recordingUploadUrl via RecordingService.
 *
 * The orchestrator only ever calls close(key) (graceful teardown) — the robust assembly trigger,
 * since the live Stop race routinely loses the trailing MediaRecorder chunk (the WS closes before
 * it flushes). is_final is the prompt-path optimization. (See @vexa/recording's assembler doc.)
 *
 * The assembler itself is PURE (in-memory, callback-only) → L2/L3-testable without disk/socket
 * (recording.test.ts). The upload leg is L4-gated (needs a live meeting-api receiver).
 */
import {
  createRecordingAssembler,
  RecordingService,
  type RecordingMaster,
  type RecordingMasterFormat,
} from '@vexa/recording';
import type { Invocation } from './config.js';
import type { RecordingSink } from './ports.js';

/** The RecordingSink extended with the chunk ingress the capture bridge's MediaRecorder tap
 *  pumps into. The orchestrator only sees close(key); the bridge holds the BotRecordingSink to
 *  feed chunks as they arrive from the page-side recorder. */
export interface BotRecordingSink extends RecordingSink {
  /** One recording.v1 chunk for `key`: monotonic seq, the COMPLETED-signal flag, format, bytes. */
  chunk(key: string, seq: number, isFinal: boolean, format: RecordingMasterFormat, bytes: Uint8Array): Promise<void>;
  /** Permanently discard later chunks/finalization before a retention stop flushes the pipeline. */
  revoke(): void;
}

export interface RecordingSinkOptions {
  inv: Invocation;
  /** Override the master handler (tests inject this to assert the assembled master without HTTP).
   *  Default = upload to inv.recordingUploadUrl via RecordingService. */
  onMaster?: (master: RecordingMaster) => void | Promise<void>;
  /** Adapter seam for testing the default upload path without touching disk/network. */
  recordingServiceFactory?: (
    meetingId: number | string,
    sessionUid: string,
  ) => Pick<RecordingService, 'writeBlob' | 'upload' | 'cleanup'>;
  /** Managed recording.v1 transport seam. The default is RecordingService.uploadChunk(). */
  recordingChunkServiceFactory?: (
    meetingId: number | string,
    sessionUid: string,
  ) => Pick<RecordingService, 'uploadChunk'>;
  /** Hard worker-side queue bounds. Normal page binding backpressure keeps this at one. */
  maxPendingChunks?: number;
  maxPendingBytes?: number;
  log?: (msg: string) => void;
}

/** Upload an assembled master to meeting-api. The 0.11 RecordingService.upload() POSTs the
 *  finalized file to the internal upload endpoint (multipart, retry/backoff). We write the master
 *  bytes through writeBlob (it owns the temp file) then upload to inv.recordingUploadUrl. Best-
 *  effort: an upload failure is logged, never thrown (the orchestrator's teardown must not hang). */
async function uploadMaster(
  inv: Invocation,
  master: RecordingMaster,
  log: (m: string) => void,
  serviceFactory: RecordingSinkOptions['recordingServiceFactory'] =
    (meetingId, sessionUid) => new RecordingService(meetingId, sessionUid),
): Promise<void> {
  const url = inv.recordingUploadUrl;
  if (!url) { log(`recording: no recordingUploadUrl — master (${master.bytes.length}B, ${master.chunks} chunks) NOT uploaded`); return; }
  try {
    const meetingId = inv.meeting_id ?? 0;
    const sessionUid = inv.connectionId ?? inv.nativeMeetingId ?? master.key;
    const svc = serviceFactory(meetingId, sessionUid);
    await svc.writeBlob(Buffer.from(master.bytes), master.format);
    await svc.upload(url, inv.token ?? '');
    await svc.cleanup().catch(() => { /* best-effort */ });
    log(`recording: uploaded master ${master.key} (${master.bytes.length}B, ${master.chunks} chunks, ${master.format})`);
  } catch (e) {
    log(`recording: upload FAILED for ${master.key}: ${String(e)}`);
  }
}

/**
 * Build the recording sink. Accumulates chunks per key; on the is_final chunk OR close(key)
 * assembles the master and hands it to onMaster (default: upload to meeting-api).
 */
export function createBotRecordingSink(opts: RecordingSinkOptions): BotRecordingSink {
  const log = opts.log ?? (() => { /* silent by default */ });
  // Desktop/tests and ordinary invocation.v1 retain the historical in-memory assembler. The
  // managed invocation.v2 path must never retain a meeting-long master in an ephemeral worker:
  // every MediaRecorder timeslice is durably uploaded before the page binding ACKs it.
  if (opts.onMaster || opts.inv.contractVersion !== 'invocation.v2') {
    const onMaster = opts.onMaster ?? ((master: RecordingMaster) => uploadMaster(
      opts.inv,
      master,
      log,
      opts.recordingServiceFactory,
    ));
    let revoked = false;
    let failure: unknown;
    const pending = new Set<Promise<void>>();
    const assembler = createRecordingAssembler({
      onMaster: (master) => {
        if (revoked) return;
        const task = Promise.resolve(onMaster(master))
          .catch((error) => { failure = error; })
          .finally(() => { pending.delete(task); });
        pending.add(task);
      },
      log,
    });
    const waitForMaster = async (): Promise<void> => {
      await Promise.all([...pending]);
      if (failure) throw failure;
    };
    return {
      async chunk(key, seq, isFinal, format, bytes) {
        if (revoked) return;
        assembler.chunk(key, seq, isFinal, format, bytes);
        if (isFinal) await waitForMaster();
      },
      async close(key) {
        if (revoked) return;
        assembler.close(key);
        await waitForMaster();
      },
      revoke() { revoked = true; },
    };
  }

  const uploadUrl = opts.inv.recordingUploadUrl;
  if (!uploadUrl) throw new Error('managed recording requires recordingUploadUrl');
  const meetingId = opts.inv.meeting_id ?? 0;
  const sessionUid = opts.inv.connectionId ?? opts.inv.nativeMeetingId ?? 'session';
  const service = (opts.recordingChunkServiceFactory
    ?? ((id, uid) => new RecordingService(id, uid)))(meetingId, sessionUid);
  const maxPendingChunks = opts.maxPendingChunks ?? 4;
  const maxPendingBytes = opts.maxPendingBytes ?? 16 * 1024 * 1024;
  if (!Number.isSafeInteger(maxPendingChunks) || maxPendingChunks < 1) {
    throw new Error('maxPendingChunks must be a positive integer');
  }
  if (!Number.isSafeInteger(maxPendingBytes) || maxPendingBytes < 1) {
    throw new Error('maxPendingBytes must be a positive integer');
  }

  let revoked = false;
  let fatal: unknown;
  let expectedSeq = 0;
  let sessionKey: string | undefined;
  let mediaFormat: RecordingMasterFormat | undefined;
  let sawChunk = false;
  let finalUploaded = false;
  let pendingChunks = 0;
  let pendingBytes = 0;
  let currentAbort: AbortController | undefined;
  let tail: Promise<void> = Promise.resolve();
  let closePromise: Promise<void> | undefined;

  const asError = (error: unknown): Error => (
    error instanceof Error ? error : new Error('recording chunk upload failed')
  );

  const enqueue = (
    key: string,
    seq: number,
    isFinal: boolean,
    format: RecordingMasterFormat,
    bytes: Uint8Array,
    internalFinal = false,
  ): Promise<void> => {
    if (revoked) return Promise.resolve();
    if (fatal) return Promise.reject(asError(fatal));
    if (!Number.isSafeInteger(seq) || seq < 0) {
      fatal = new Error('recording chunk sequence is invalid');
      return Promise.reject(fatal);
    }
    if (finalUploaded && !internalFinal) {
      fatal = new Error('recording received content after its final signal');
      return Promise.reject(fatal);
    }
    if (sessionKey !== undefined && sessionKey !== key) {
      fatal = new Error('recording sink cannot mix session keys');
      return Promise.reject(fatal);
    }
    if (mediaFormat !== undefined && mediaFormat !== format) {
      fatal = new Error('recording format changed during capture');
      return Promise.reject(fatal);
    }
    const body = Buffer.from(bytes);
    if (pendingChunks + 1 > maxPendingChunks || pendingBytes + body.length > maxPendingBytes) {
      fatal = new Error('recording upload backpressure limit exceeded');
      currentAbort?.abort();
      return Promise.reject(fatal);
    }
    sessionKey = key;
    mediaFormat = format;
    sawChunk = true;
    pendingChunks++;
    pendingBytes += body.length;

    const task = tail.then(async () => {
      if (revoked) return;
      if (fatal) throw asError(fatal);
      if (seq !== expectedSeq) {
        throw new Error(`recording chunk sequence gap: expected ${expectedSeq}`);
      }
      const controller = new AbortController();
      currentAbort = controller;
      try {
        await service.uploadChunk(
          uploadUrl,
          opts.inv.token ?? '',
          body,
          seq,
          isFinal,
          format,
          controller.signal,
        );
      } finally {
        if (currentAbort === controller) currentAbort = undefined;
      }
      if (revoked) return;
      expectedSeq = seq + 1;
      if (isFinal) finalUploaded = true;
      log(`recording: durably uploaded chunk ${seq}${isFinal ? ' (final)' : ''}`);
    }).catch((error) => {
      if (revoked) return;
      fatal = asError(error);
      throw fatal;
    }).finally(() => {
      pendingChunks--;
      pendingBytes -= body.length;
    });
    // Keep the ordering chain live after a caller observes a rejection; the latched `fatal` still
    // prevents any later body from being sent.
    tail = task.then(() => undefined, () => undefined);
    return task;
  };

  return {
    chunk(key, seq, isFinal, format, bytes) {
      return enqueue(key, seq, isFinal, format, bytes);
    },
    close(key) {
      if (closePromise) return closePromise;
      closePromise = (async () => {
        await tail;
        if (revoked) return;
        if (fatal) throw asError(fatal);
        if (!sawChunk || finalUploaded) return;
        // A page crash can lose MediaRecorder's empty final callback. Close supplies the next
        // contiguous signal only after every prior chunk has a durable ACK.
        await enqueue(key, expectedSeq, true, mediaFormat ?? 'webm', new Uint8Array(0), true);
      })();
      return closePromise;
    },
    revoke() {
      if (revoked) return;
      revoked = true;
      currentAbort?.abort();
    },
  };
}
