/** Managed invocation.v2 transcript egress over the MeetingToken-authenticated meeting-api edge. */
import type { TranscriptSegment } from '../contracts.js';
import type { RedisCarrierFenceClient, RevocableTranscriptSink } from './transcript-redis.js';
import { TranscriptWriteFenced } from './transcript-redis.js';

const MAX_REQUEST_BYTES = 64 * 1024;

export interface HttpResponseLike {
  ok: boolean;
  status: number;
  body?: { cancel(): Promise<void> | void } | null;
}

export type ManagedFetchLike = (
  url: string,
  init: {
    method: 'POST';
    headers: Record<string, string>;
    body: string;
    redirect: 'error';
    signal: AbortSignal;
  },
) => Promise<HttpResponseLike>;

export interface HttpTranscriptSinkOptions {
  transcriptIngestUrl: string;
  retentionFenceUrl: string;
  token: string;
  connectionId: string;
  fetchImpl?: ManagedFetchLike;
  attempts?: number;
  timeoutMs?: number;
  backoffMs?: number;
  sleep?: (ms: number) => Promise<void>;
}

export type ManagedHttpTranscriptClient = RevocableTranscriptSink & RedisCarrierFenceClient;

const realSleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

export function createHttpTranscriptSink(opts: HttpTranscriptSinkOptions): ManagedHttpTranscriptClient {
  const {
    transcriptIngestUrl,
    retentionFenceUrl,
    token,
    connectionId,
    fetchImpl = globalThis.fetch as unknown as ManagedFetchLike,
    attempts = 3,
    timeoutMs = 5_000,
    backoffMs = 100,
    sleep = realSleep,
  } = opts;
  if (!transcriptIngestUrl || !retentionFenceUrl || !token || !connectionId) {
    throw new Error('managed transcript HTTP configuration is incomplete');
  }
  const headers = {
    'content-type': 'application/json',
    authorization: `Bearer ${token}`,
  };
  let revoked = false;

  async function post(url: string, payload: object): Promise<void> {
    const body = JSON.stringify(payload);
    if (Buffer.byteLength(body, 'utf8') > MAX_REQUEST_BYTES) {
      throw new Error('managed transcript request exceeds the 64KiB limit');
    }
    let lastError = 'request failed';
    for (let attempt = 1; attempt <= Math.max(1, attempts); attempt++) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await fetchImpl(url, {
          method: 'POST', headers, body, redirect: 'error', signal: controller.signal,
        });
        try {
          await response.body?.cancel();
        } catch {
          /* best-effort bounded disposal */
        }
        if (response.ok) return;
        if (response.status === 409) throw new TranscriptWriteFenced();
        lastError = `HTTP ${response.status}`;
        if (response.status < 500 && response.status !== 429) {
          throw new Error(`managed transcript request rejected (${lastError})`);
        }
      } catch (error) {
        if (error instanceof TranscriptWriteFenced) throw error;
        lastError = (error as Error)?.message ?? String(error);
        if (lastError.startsWith('managed transcript request rejected')) throw error;
      } finally {
        clearTimeout(timer);
      }
      if (attempt < Math.max(1, attempts)) await sleep(backoffMs * 2 ** (attempt - 1));
    }
    throw new Error(`managed transcript request failed after bounded retry: ${lastError}`);
  }

  return {
    async publish(segment: TranscriptSegment): Promise<void> {
      if (revoked) throw new TranscriptWriteFenced();
      await post(transcriptIngestUrl, { connection_id: connectionId, segment });
    },
    revoke(): void { revoked = true; },
    async fenceMeetingCarriers(call): Promise<void> {
      if (!call.raw || !call.processed) {
        throw new Error('managed deadline must fence raw and processed carriers together');
      }
      await post(retentionFenceUrl, {
        connection_id: connectionId,
        raw: true,
        processed: true,
      });
      revoked = true;
    },
  };
}
