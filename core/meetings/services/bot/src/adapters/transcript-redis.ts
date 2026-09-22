/**
 * transcript.v1 egress ADAPTER — redis stream + pub/sub.
 *
 * Implements the `TranscriptSink` port. On each confirmed segment the engine pushes, this
 * fans out to BOTH legs of the 0.11 transcript transport:
 *
 *   1. STREAM  `transcription_segments`  (XADD * { payload })  — the durable feed the collector
 *      [Py] consumes. `payload` is JSON `{ type: 'transcription', ...segment }` (the segment
 *      fields spread alongside the discriminator, per the 0.11 collector wire format).
 *   2. PUB/SUB `tc:meeting:{meetingId}:mutable`  — the live mutable channel the gateway forwards
 *      to the dashboard. Message is JSON `{ type: 'transcript', meeting: { id }, segment }`.
 *
 * L3-testable via an INJECTED minimal `client` ({ xAdd, publish }) — no real redis. The factory
 * `redisClientFrom(url)` wraps node-redis v4 into that minimal interface for the composition root.
 */
import { createClient } from 'redis';
import type { TranscriptSegment } from '../contracts.js';
import type { TranscriptSink } from '../ports.js';

/** The redis stream the collector consumes (durable transcript.v1 feed). */
export const TRANSCRIPTION_STREAM = 'transcription_segments';
/** Approximate producer-side storage cap for the global source stream. Two consumer groups read it,
 * so a collector-side XDEL after one ACK would race the agent watcher. This is defense-in-depth,
 * not a lossless guarantee: under extreme lag, trimming can evict an unread entry. Exact lifecycle
 * erasure uses terminal/quiescence checks plus a meeting-targeted purge in meeting-api. */
export const TRANSCRIPTION_STREAM_MAXLEN = 100_000;

/** Permanent per-row fence installed by Minutes retention before carrier deletion. */
export const retentionFenceKey = (meetingId: string | number): string =>
  `zaki:retention:meeting:${meetingId}:fence`;

const WRITE_TRANSCRIPT_IF_UNFENCED = `
if redis.call('HGET', KEYS[1], 'raw') == '1' then
  return 0
end
local cutoff_ms = tonumber(ARGV[5])
if cutoff_ms ~= nil then
  local redis_time = redis.call('TIME')
  local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
  if now_ms >= cutoff_ms then
    return 0
  end
end
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[1], '*', 'payload', ARGV[2])
redis.call('PUBLISH', ARGV[3], ARGV[4])
return 1
`;

const FENCE_MEETING_CARRIERS = `
if ARGV[1] == '1' then
  redis.call('HSET', KEYS[1], 'raw', '1')
end
if ARGV[2] == '1' then
  redis.call('HSET', KEYS[1], 'processed', '1')
end
redis.call('PERSIST', KEYS[1])
return 1
`;

export class TranscriptWriteFenced extends Error {
  constructor() {
    super('meeting transcript writes are fenced');
    this.name = 'TranscriptWriteFenced';
  }
}

/** The live mutable pub/sub channel the gateway forwards to the dashboard. */
export const mutableChannel = (meetingId: string | number): string => `tc:meeting:${meetingId}:mutable`;

/** The minimal redis surface the sink needs — injected so the adapter is offline-provable. */
export interface RedisTranscriptClient {
  /** XADD key id fields — the live impl forwards to node-redis `xAdd`. */
  xAdd(
    key: string,
    id: string,
    fields: Record<string, string>,
    options?: {
      TRIM?: {
        strategy?: 'MAXLEN' | 'MINID';
        strategyModifier?: '=' | '~';
        threshold: number;
        limit?: number;
      };
    },
  ): Promise<unknown>;
  /** PUBLISH channel message. */
  publish(channel: string, message: string): Promise<unknown>;
  /** Atomically check the row's raw-retention fence, then XADD + PUBLISH both egress legs. */
  writeTranscriptIfWritable(call: {
    fenceKey: string;
    sourceStream: string;
    payload: string;
    channel: string;
    message: string;
    maxLen: number;
    /** Optional Redis-authoritative absolute cutoff for commands queued before local revocation. */
    captureExpiresAtMs?: number;
  }): Promise<boolean>;
}

export interface RedisCarrierFenceClient {
  /** Monotonically HSET selected retention scopes and remove any accidental legacy TTL. */
  fenceMeetingCarriers(call: {
    fenceKey: string;
    raw: boolean;
    processed: boolean;
  }): Promise<void>;
}

export async function fenceAllMeetingCarriers(
  client: RedisCarrierFenceClient,
  meetingId: string | number,
): Promise<void> {
  await client.fenceMeetingCarriers({
    fenceKey: retentionFenceKey(meetingId),
    raw: true,
    processed: true,
  });
}

export interface RedisTranscriptSinkOptions {
  client: RedisTranscriptClient;
  /** The meeting id used in the mutable channel + bundle envelope. */
  meetingId: string | number;
  /** The native meeting code (e.g. `abc-defg-hij`). Stamped on the segment so the agent watcher keys
   *  on the native id WITHOUT a /meetings lookup (P23: one writer, no re-derivation). */
  nativeMeetingId?: string;
  /** Absolute managed-capture cutoff; ordinary upstream workloads omit it. */
  captureExpiresAt?: string;
}

/** Transcript sink with a one-way in-process latch used before a remote retention fence. */
export interface RevocableTranscriptSink extends TranscriptSink {
  revoke(): void;
}

/** Build the live transcript sink. `publish` XADDs the durable feed AND publishes the live
 *  mutable channel for one segment (best-effort fan-out; rejections propagate to the engine,
 *  which decides whether a publish failure is fatal). */
export function createRedisTranscriptSink(opts: RedisTranscriptSinkOptions): RevocableTranscriptSink {
  const { client, meetingId, nativeMeetingId } = opts;
  const channel = mutableChannel(meetingId);
  let revoked = false;
  let captureExpiresAtMs: number | undefined;
  if (opts.captureExpiresAt !== undefined) {
    captureExpiresAtMs = Date.parse(opts.captureExpiresAt);
    if (!Number.isFinite(captureExpiresAtMs)) {
      throw new Error('capture retention deadline is invalid');
    }
  }

  async function publish(segment: TranscriptSegment): Promise<void> {
    if (revoked) throw new TranscriptWriteFenced();
    // Leg 1: durable stream → collector. The collector's `ingest` REQUIRES the envelope
    // `{ type, meeting_id, segments:[…] }` — meeting_id to route the segment to its meeting, a
    // `segments` LIST to drain (a payload missing either is silently dropped: ingest.py `return 0`).
    // Emit that, not a flat segment, so the bot's transcripts actually reach the collector. (The
    // mock-bot L3 lane caught the flat form: O6 read the raw stream directly and never exercised the collector.)
    const payload = JSON.stringify({
      type: 'transcription', meeting_id: meetingId, native_meeting_id: nativeMeetingId, segments: [segment],
    });
    // Leg 2: live mutable channel → gateway → dashboard. Both legs share one Redis command:
    // whichever command wins first, a producer append is either purged later or refused forever.
    const msg = JSON.stringify({ type: 'transcript', meeting: { id: meetingId }, segment });
    const accepted = await client.writeTranscriptIfWritable({
      fenceKey: retentionFenceKey(meetingId),
      sourceStream: TRANSCRIPTION_STREAM,
      payload,
      channel,
      message: msg,
      maxLen: TRANSCRIPTION_STREAM_MAXLEN,
      captureExpiresAtMs,
    });
    if (!accepted) throw new TranscriptWriteFenced();
  }

  return {
    publish,
    // Synchronous and monotonic: pipeline.stop() may immediately flush after this returns.
    revoke() { revoked = true; },
  };
}

/** A live transcript client that also exposes connect/quit so the composition root can
 *  lazily connect and tear down. */
export type LiveRedisTranscriptClient = RedisTranscriptClient & RedisCarrierFenceClient & {
  connect(): Promise<void>;
  quit(): Promise<void>;
};

/** Wrap node-redis v4 (`createClient`) into the minimal `RedisTranscriptClient`. Lazily
 *  connects on first use so the composition root can construct it before redis is reachable
 *  (the connection error surfaces on the first publish, not at construction). */
export function redisClientFrom(redisUrl: string): LiveRedisTranscriptClient {
  const client = createClient({ url: redisUrl });
  // node-redis emits 'error' events; without a listener an unreachable server throws unhandled.
  client.on('error', (err: unknown) => {
    console.error(`[bot] redis (transcript) error: ${(err as Error)?.message ?? String(err)}`);
  });
  let connected = false;
  const ensure = async (): Promise<void> => {
    if (!connected) {
      await client.connect();
      connected = true;
    }
  };
  return {
    async xAdd(key, id, fields, options) {
      await ensure();
      return client.xAdd(key, id, fields, options);
    },
    async publish(channel, message) {
      await ensure();
      return client.publish(channel, message);
    },
    async writeTranscriptIfWritable(call) {
      await ensure();
      const result = await client.eval(WRITE_TRANSCRIPT_IF_UNFENCED, {
        keys: [call.fenceKey, call.sourceStream],
        arguments: [
          String(call.maxLen),
          call.payload,
          call.channel,
          call.message,
          call.captureExpiresAtMs === undefined ? '' : String(call.captureExpiresAtMs),
        ],
      });
      return Number(result) === 1;
    },
    async fenceMeetingCarriers(call) {
      await ensure();
      await client.eval(FENCE_MEETING_CARRIERS, {
        keys: [call.fenceKey],
        arguments: [call.raw ? '1' : '0', call.processed ? '1' : '0'],
      });
    },
    async connect() {
      await ensure();
    },
    async quit() {
      if (connected) {
        await client.quit();
        connected = false;
      }
    },
  };
}
