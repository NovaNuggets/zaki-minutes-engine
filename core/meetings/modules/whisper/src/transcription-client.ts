import { randomBytes } from 'node:crypto';

import { log } from './log.js';
import { isLowConfidenceSegment } from './confidence.js';

export interface TranscriptionWord {
  word: string;
  start: number;
  end: number;
  probability: number;
}

export interface TranscriptionSegment {
  start: number;
  end: number;
  text: string;
  avg_logprob?: number;
  no_speech_prob?: number;
  compression_ratio?: number;
  words?: TranscriptionWord[];
}

export interface TranscriptionResult {
  text: string;
  language: string;
  language_probability?: number;
  duration: number;
  segments: TranscriptionSegment[];
}

export interface TranscriptionClientConfig {
  /** Base URL of transcription-service, e.g. "http://localhost:8083" */
  serviceUrl: string;
  /** Optional bearer token for authentication */
  apiToken?: string;
  /** Max retry attempts for transient failures. Default: 3 */
  maxRetries?: number;
  /** Base delay between retries in ms. Default: 1000 */
  retryDelayMs?: number;
  /** Sample rate of input audio. Default: 16000 */
  sampleRate?: number;
  /** Max speech segment duration in seconds. Whisper forces a segment split at this length.
   *  Lower values = more frequent confirmations = faster output. Default: server default (15s) */
  maxSpeechDurationSec?: number;
  /** Minimum silence duration (ms) for VAD to split segments. Lower = more splits at natural pauses.
   *  Default: server default (160ms). Use ~100ms for more granular segments. */
  minSilenceDurationMs?: number;
}

/** Hosted STT is an untrusted boundary. A 15-second verbose transcript is normally tiny; this
 * ceiling leaves ample headroom while preventing an unbounded `response.json()` allocation. */
export const MAX_TRANSCRIPTION_RESPONSE_BYTES = 8 * 1024 * 1024;
export const MAX_TRANSCRIPTION_AUDIO_SECONDS = 120;
export const MAX_TRANSCRIPTION_PROMPT_CHARS = 8_000;
const MAX_TRANSCRIPTION_AUDIO_SAMPLES = 2_000_000; // <= 4 MiB of encoded mono PCM
const TRANSCRIPTION_TIME_TOLERANCE_SECONDS = 5;
const MAX_TRANSCRIPTION_TEXT_CHARS = 1_000_000;
const MAX_TRANSCRIPTION_SEGMENTS = 20_000;
const MAX_TRANSCRIPTION_WORDS = 100_000;
const MAX_TRANSCRIPTION_WORD_CHARS = 1_000;
const MAX_LANGUAGE_CHARS = 64;

interface RawTranscriptionWord {
  word: string;
  start?: number;
  end?: number;
  probability?: number;
}

interface RawTranscriptionSegment {
  start?: number;
  end?: number;
  text?: string;
  avg_logprob?: number;
  no_speech_prob?: number;
  compression_ratio?: number;
  words?: RawTranscriptionWord[];
}

interface RawTranscriptionResponse {
  text?: string;
  language?: string;
  language_probability?: number;
  duration?: number;
  segments?: RawTranscriptionSegment[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function isFiniteWhenPresent(value: unknown): value is number | undefined {
  return value === undefined || (typeof value === 'number' && Number.isFinite(value));
}

function isTimestampWhenPresent(value: unknown, maxTimelineSeconds: number): value is number | undefined {
  return value === undefined
    || (typeof value === 'number' && Number.isFinite(value)
      && value >= 0 && value <= maxTimelineSeconds);
}

/** Validate every field this adapter reads or returns and bound attacker-controlled fanout. */
export function validTranscriptionResponse(
  value: unknown,
  maxTimelineSeconds = MAX_TRANSCRIPTION_AUDIO_SECONDS + TRANSCRIPTION_TIME_TOLERANCE_SECONDS,
): value is RawTranscriptionResponse {
  if (!isRecord(value)) return false;
  if (!Number.isFinite(maxTimelineSeconds) || maxTimelineSeconds < 0) return false;
  if (value.text !== undefined
      && (typeof value.text !== 'string' || value.text.length > MAX_TRANSCRIPTION_TEXT_CHARS)) return false;
  if (value.language !== undefined
      && (typeof value.language !== 'string' || value.language.length > MAX_LANGUAGE_CHARS)) return false;
  if (!isFiniteWhenPresent(value.language_probability)
      || !isTimestampWhenPresent(value.duration, maxTimelineSeconds)) return false;
  if (value.segments === undefined) return true;
  if (!Array.isArray(value.segments) || value.segments.length > MAX_TRANSCRIPTION_SEGMENTS) return false;

  let wordCount = 0;
  for (const rawSegment of value.segments) {
    if (!isRecord(rawSegment)) return false;
    if (!isTimestampWhenPresent(rawSegment.start, maxTimelineSeconds)
        || !isTimestampWhenPresent(rawSegment.end, maxTimelineSeconds)
        || !isFiniteWhenPresent(rawSegment.avg_logprob)
        || !isFiniteWhenPresent(rawSegment.no_speech_prob)
        || !isFiniteWhenPresent(rawSegment.compression_ratio)) return false;
    if (rawSegment.start !== undefined && rawSegment.end !== undefined
        && rawSegment.end < rawSegment.start) return false;
    if (rawSegment.text !== undefined
        && (typeof rawSegment.text !== 'string'
          || rawSegment.text.length > MAX_TRANSCRIPTION_TEXT_CHARS)) return false;
    if (rawSegment.words === undefined) continue;
    if (!Array.isArray(rawSegment.words)) return false;
    wordCount += rawSegment.words.length;
    if (wordCount > MAX_TRANSCRIPTION_WORDS) return false;
    for (const rawWord of rawSegment.words) {
      if (!isRecord(rawWord)
          || typeof rawWord.word !== 'string'
          || rawWord.word.length > MAX_TRANSCRIPTION_WORD_CHARS
          || !isTimestampWhenPresent(rawWord.start, maxTimelineSeconds)
          || !isTimestampWhenPresent(rawWord.end, maxTimelineSeconds)
          || !isFiniteWhenPresent(rawWord.probability)) return false;
      if (rawWord.start !== undefined && rawWord.end !== undefined
          && rawWord.end < rawWord.start) return false;
    }
  }
  return true;
}

/** Resolve every supported STT setting shape to the one OpenAI-compatible transcription path.
 * Operators and the Settings probe may provide the service root, `/v1`, `/v1/audio`, or the
 * complete endpoint; suffix-aware completion prevents `/v1/v1/audio/transcriptions`. */
export function canonicalTranscriptionEndpoint(serviceUrl: string): string {
  const base = serviceUrl.trim().replace(/\/+$/, '');
  if (base.endsWith('/v1/audio/transcriptions')) return base;
  if (base.endsWith('/v1/audio')) return `${base}/transcriptions`;
  if (base.endsWith('/v1')) return `${base}/audio/transcriptions`;
  return `${base}/v1/audio/transcriptions`;
}

/** The STT boundary's FAILURE vocabulary (P5 + P18: an adapter must translate the
 *  dependency's failures, not just its successes). A consumer reads `.kind` to surface
 *  an attributable health event instead of silently degrading to "no transcript". */
export type TranscriptionFaultKind =
  | 'payment_required'   // 402 — out of balance / credits exhausted
  | 'unauthorized'       // 401 / 403 — bad or expired token
  | 'rate_limited'       // 429
  | 'unavailable'        // 5xx or network error
  | 'timeout'            // request aborted (no response in time)
  | 'bad_request'        // other 4xx
  | 'unknown';

/** A typed STT failure. `source` lets a consumer attribute it; `retryable` drives backoff. */
export class TranscriptionError extends Error {
  readonly source = 'stt' as const;
  constructor(
    readonly kind: TranscriptionFaultKind,
    readonly status: number | undefined,
    readonly detail: string | undefined,
    readonly retryable: boolean,
  ) {
    super(`stt ${kind}${status ? ` (HTTP ${status})` : ''}${detail ? `: ${detail}` : ''}`);
    this.name = 'TranscriptionError';
  }
}

/** Map an HTTP status to a typed fault (the anti-corruption translation, P5). */
function classifyHttp(status: number, detail?: string): TranscriptionError {
  if (status === 402) return new TranscriptionError('payment_required', status, detail, false);
  if (status === 401 || status === 403) return new TranscriptionError('unauthorized', status, detail, false);
  if (status === 429) return new TranscriptionError('rate_limited', status, detail, true);
  if (status >= 500) return new TranscriptionError('unavailable', status, detail, true);
  if (status >= 400) return new TranscriptionError('bad_request', status, detail, false);
  return new TranscriptionError('unknown', status, detail, false);
}

function invalidResponse(): TranscriptionError {
  // This text is local and stable. Never attach the untrusted response body: some gateways echo
  // the Authorization header into their error payloads.
  return new TranscriptionError('unknown', undefined, 'invalid response', false);
}

async function readBoundedResponse(response: Response): Promise<Uint8Array> {
  const declared = response.headers.get('content-length');
  if (declared !== null) {
    const length = Number(declared);
    if (Number.isFinite(length) && length > MAX_TRANSCRIPTION_RESPONSE_BYTES) {
      try { await response.body?.cancel('response body exceeds limit'); } catch { /* best effort */ }
      throw invalidResponse();
    }
  }
  if (!response.body) return new Uint8Array();

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      total += value.byteLength;
      if (total > MAX_TRANSCRIPTION_RESPONSE_BYTES) {
        try { await reader.cancel('response body exceeds limit'); } catch { /* best effort */ }
        throw invalidResponse();
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

/**
 * HTTP client for the transcription-service.
 * Converts Float32Array audio to WAV, sends as multipart form,
 * and returns transcription results.
 */
export class TranscriptionClient {
  private serviceUrl: string;
  private apiToken: string | undefined;
  private maxRetries: number;
  private retryDelayMs: number;
  private sampleRate: number;
  private maxSpeechDurationSec: number | undefined;
  private minSilenceDurationMs: number | undefined;
  constructor(config: TranscriptionClientConfig) {
    const sampleRate = config.sampleRate ?? 16000;
    if (!Number.isFinite(sampleRate) || sampleRate <= 0 || sampleRate > 192_000) {
      throw new TypeError('sampleRate must be a positive finite value at or below 192000');
    }
    const maxRetries = config.maxRetries ?? 3;
    if (!Number.isInteger(maxRetries) || maxRetries < 0 || maxRetries > 10) {
      throw new TypeError('maxRetries must be an integer between 0 and 10');
    }
    const retryDelayMs = config.retryDelayMs ?? 1000;
    if (!Number.isFinite(retryDelayMs) || retryDelayMs < 0 || retryDelayMs > 60_000) {
      throw new TypeError('retryDelayMs must be finite and between 0 and 60000');
    }
    if (config.maxSpeechDurationSec !== undefined
        && (!Number.isFinite(config.maxSpeechDurationSec)
          || config.maxSpeechDurationSec <= 0
          || config.maxSpeechDurationSec > MAX_TRANSCRIPTION_AUDIO_SECONDS)) {
      throw new TypeError(`maxSpeechDurationSec must be finite and between 0 and ${MAX_TRANSCRIPTION_AUDIO_SECONDS}`);
    }
    if (config.minSilenceDurationMs !== undefined
        && (!Number.isFinite(config.minSilenceDurationMs)
          || config.minSilenceDurationMs < 0
          || config.minSilenceDurationMs > 60_000)) {
      throw new TypeError('minSilenceDurationMs must be finite and between 0 and 60000');
    }
    this.serviceUrl = canonicalTranscriptionEndpoint(config.serviceUrl);
    this.apiToken = config.apiToken;
    this.maxRetries = maxRetries;
    this.retryDelayMs = retryDelayMs;
    this.sampleRate = sampleRate;
    this.maxSpeechDurationSec = config.maxSpeechDurationSec;
    this.minSilenceDurationMs = config.minSilenceDurationMs;
  }

  /**
   * Transcribe a Float32Array audio buffer.
   * Converts to WAV, POSTs to transcription-service, returns parsed result.
   * Retries on transient failures (503, network errors).
   */
  async transcribe(audioData: Float32Array, language?: string, prompt?: string): Promise<TranscriptionResult> {
    const maxSamples = Math.min(
      MAX_TRANSCRIPTION_AUDIO_SAMPLES,
      Math.floor(this.sampleRate * MAX_TRANSCRIPTION_AUDIO_SECONDS),
    );
    if (audioData.length > maxSamples) {
      throw new TranscriptionError('bad_request', undefined, 'audio exceeds limit', false);
    }
    if (language && (language.length > MAX_LANGUAGE_CHARS || /[\r\n]/.test(language))) {
      throw new TranscriptionError('bad_request', undefined, 'invalid language', false);
    }
    const boundedPrompt = prompt?.slice(0, MAX_TRANSCRIPTION_PROMPT_CHARS);
    const wavBuffer = this.float32ToWav(audioData);
    const audioDurationSeconds = audioData.length / this.sampleRate;

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        const result = await this.sendRequest(wavBuffer, audioDurationSeconds, language, boundedPrompt);
        return result;
      } catch (err: any) {
        // Normalize anything non-HTTP (abort/network) into a typed fault too, so the
        // thrown value is ALWAYS a TranscriptionError the consumer can attribute (P18).
        const fault: TranscriptionError = err instanceof TranscriptionError
          ? err
          : new TranscriptionError(err?.name === 'AbortError' ? 'timeout' : 'unavailable', undefined, err?.message, true);
        const isLastAttempt = attempt === this.maxRetries;

        if (fault.retryable && !isLastAttempt) {
          const delay = this.retryDelayMs * Math.pow(2, attempt);
          log(`[TranscriptionClient] ${fault.kind} (attempt ${attempt + 1}/${this.maxRetries + 1}): ${fault.message}. Retrying in ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }

        // Non-retryable (402/401/4xx) or retries exhausted → surface the typed fault.
        log(`[TranscriptionClient] transcription failed after ${attempt + 1} attempt(s): ${fault.message}`);
        throw fault;
      }
    }

    // Should never reach here, but TypeScript needs it
    throw new Error('Transcription failed: exhausted retries');
  }

  /**
   * Send the WAV buffer to the transcription-service as multipart form data.
   */
  private async sendRequest(
    wavBuffer: Buffer,
    audioDurationSeconds: number,
    language?: string,
    prompt?: string,
  ): Promise<TranscriptionResult> {
    // Build multipart form data manually (no external dependency needed)
    // Transcript-derived prompt text is participant-controlled. A cryptographically random
    // boundary prevents it from forging a delimiter and injecting a sibling multipart field.
    const boundary = `----FormBoundary${randomBytes(18).toString('hex')}`;

    const parts: Buffer[] = [];

    // File part
    parts.push(Buffer.from(
      `--${boundary}\r\n` +
      `Content-Disposition: form-data; name="file"; filename="audio.wav"\r\n` +
      `Content-Type: audio/wav\r\n\r\n`
    ));
    parts.push(wavBuffer);
    parts.push(Buffer.from('\r\n'));

    // Model part (required by OpenAI-compatible API)
    parts.push(Buffer.from(
      `--${boundary}\r\n` +
      `Content-Disposition: form-data; name="model"\r\n\r\n` +
      `whisper-1\r\n`
    ));

    // Response format part
    parts.push(Buffer.from(
      `--${boundary}\r\n` +
      `Content-Disposition: form-data; name="response_format"\r\n\r\n` +
      `verbose_json\r\n`
    ));

    // Language part (if specified)
    if (language) {
      parts.push(Buffer.from(
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="language"\r\n\r\n` +
        `${language}\r\n`
      ));
    }

    // Request word-level timestamps
    parts.push(Buffer.from(
      `--${boundary}\r\n` +
      `Content-Disposition: form-data; name="timestamp_granularities"\r\n\r\n` +
      `word\r\n`
    ));

    // Max speech segment duration (controls how often Whisper splits segments)
    if (this.maxSpeechDurationSec !== undefined) {
      parts.push(Buffer.from(
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="max_speech_duration_s"\r\n\r\n` +
        `${this.maxSpeechDurationSec}\r\n`
      ));
    }

    // Min silence duration for VAD segment splitting (lower = more splits at natural pauses)
    if (this.minSilenceDurationMs !== undefined) {
      parts.push(Buffer.from(
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="min_silence_duration_ms"\r\n\r\n` +
        `${this.minSilenceDurationMs}\r\n`
      ));
    }

    // Prompt: previous confirmed text as context for streaming continuity
    if (prompt) {
      parts.push(Buffer.from(
        `--${boundary}\r\n` +
        `Content-Disposition: form-data; name="prompt"\r\n\r\n` +
        `${prompt}\r\n`
      ));
    }

    // End boundary
    parts.push(Buffer.from(`--${boundary}--\r\n`));

    const body = Buffer.concat(parts);

    const headers: Record<string, string> = {
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
    };
    if (this.apiToken) {
      headers['Authorization'] = `Bearer ${this.apiToken}`;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000);

    try {
      const response = await fetch(this.serviceUrl, {
        method: 'POST',
        headers,
        body,
        signal: controller.signal,
        // Never replay meeting audio or operator credentials to a redirect target.
        // The operator-configured endpoint is the complete egress authority.
        redirect: 'error',
      });

      if (!response.ok) {
        // The status is sufficient for classification. Never allocate or reflect an untrusted
        // error body: gateways have been observed echoing Authorization in diagnostic text.
        try { await response.body?.cancel('untrusted error response'); } catch { /* best effort */ }
        throw classifyHttp(response.status);   // typed fault (P5/P18), not a bare Error
      }

      const responseBytes = await readBoundedResponse(response);
      let data: unknown;
      try {
        data = JSON.parse(new TextDecoder().decode(responseBytes));
      } catch {
        throw invalidResponse();
      }
      if (!validTranscriptionResponse(
        data,
        audioDurationSeconds + TRANSCRIPTION_TIME_TOLERANCE_SECONDS,
      )) throw invalidResponse();

      const allSegments = (data.segments ?? []).map((s) => ({
        start: s.start ?? 0,
        end: s.end ?? 0,
        text: s.text ?? '',
        avg_logprob: s.avg_logprob,
        no_speech_prob: s.no_speech_prob,
        compression_ratio: s.compression_ratio,
        words: s.words?.map((word) => ({
          word: word.word,
          start: word.start ?? 0,
          end: word.end ?? 0,
          probability: word.probability ?? 0,
        })),
      }));
      // Drop low-confidence (hallucinated / faint-bleed) segments at the source and
      // rebuild the text from what survives, so phantoms never reach the pipeline.
      // If the model returned no segments we can't score, so keep its text as-is.
      const segments = allSegments.filter((s) => !isLowConfidenceSegment(s));
      const text = allSegments.length
        ? segments.map((s) => s.text.trim()).filter(Boolean).join(' ')
        : (data.text ?? '');
      if (allSegments.length && segments.length < allSegments.length) {
        log(`[STT] dropped ${allSegments.length - segments.length}/${allSegments.length} low-confidence segment(s)`);
      }
      return {
        text,
        language: data.language || language || 'unknown',
        language_probability: data.language_probability ?? 0,
        duration: data.duration ?? 0,
        segments,
      };
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Convert Float32Array audio samples to a WAV file buffer.
   * Output: 16-bit PCM, mono, at this.sampleRate (default 16kHz).
   */
  private float32ToWav(samples: Float32Array): Buffer {
    const numChannels = 1;
    const bitsPerSample = 16;
    const bytesPerSample = bitsPerSample / 8;
    const dataSize = samples.length * bytesPerSample;
    const headerSize = 44;
    const buffer = Buffer.alloc(headerSize + dataSize);

    // RIFF header
    buffer.write('RIFF', 0);
    buffer.writeUInt32LE(36 + dataSize, 4);
    buffer.write('WAVE', 8);

    // fmt sub-chunk
    buffer.write('fmt ', 12);
    buffer.writeUInt32LE(16, 16);              // Sub-chunk size
    buffer.writeUInt16LE(1, 20);               // PCM format
    buffer.writeUInt16LE(numChannels, 22);     // Mono
    buffer.writeUInt32LE(this.sampleRate, 24);  // Sample rate
    buffer.writeUInt32LE(this.sampleRate * numChannels * bytesPerSample, 28); // Byte rate
    buffer.writeUInt16LE(numChannels * bytesPerSample, 32); // Block align
    buffer.writeUInt16LE(bitsPerSample, 34);   // Bits per sample

    // data sub-chunk
    buffer.write('data', 36);
    buffer.writeUInt32LE(dataSize, 40);

    // Convert Float32 [-1, 1] to Int16
    let offset = headerSize;
    for (let i = 0; i < samples.length; i++) {
      let sample = samples[i];
      // Clamp to [-1, 1]
      sample = Math.max(-1, Math.min(1, sample));
      // Convert to 16-bit integer
      const int16 = sample < 0 ? sample * 0x8000 : sample * 0x7FFF;
      buffer.writeInt16LE(Math.round(int16), offset);
      offset += 2;
    }

    return buffer;
  }
}
