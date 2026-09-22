/** Bounds and validation for untrusted transcription-service responses. */
export const MAX_STT_RESPONSE_BYTES = 2 * 1024 * 1024;
// Composer dictation is a short interactive window, not a meeting upload. This is roughly two
// minutes of 16 kHz mono PCM and matches the canonical meeting client's duration ceiling.
export const MAX_STT_REQUEST_BYTES = 4 * 1024 * 1024;

const MAX_TRANSCRIPT_CHARS = 200_000;
const MAX_SEGMENTS = 10_000;
const MAX_WORDS = 50_000;
const MAX_WORD_CHARS = 1_000;

export interface UpstreamWord { word?: string; start?: number; end?: number }
export interface UpstreamSegment { text?: string; words?: UpstreamWord[] }

async function readBoundedStream(
  stream: ReadableStream<Uint8Array>,
  maxBytes: number,
): Promise<Uint8Array | null> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel("body exceeds limit");
        return null;
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

/** Read at most `maxBytes` from a possibly chunked body. `null` means oversized. */
export async function readBoundedBody(req: Request, maxBytes: number): Promise<ArrayBuffer | null> {
  const declared = req.headers.get("content-length");
  if (declared !== null) {
    const length = Number(declared);
    if (Number.isFinite(length) && length > maxBytes) return null;
  }
  if (!req.body) return new ArrayBuffer(0);
  const body = await readBoundedStream(req.body, maxBytes);
  if (body === null) return null;
  const copy = new Uint8Array(body.byteLength);
  copy.set(body);
  return copy.buffer;
}

export async function readBoundedResponse(
  response: Response,
  maxBytes: number,
): Promise<Uint8Array | null> {
  const declared = response.headers.get("content-length");
  if (declared !== null) {
    const length = Number(declared);
    if (Number.isFinite(length) && length > maxBytes) {
      await response.body?.cancel("response body exceeds limit");
      return null;
    }
  }
  if (!response.body) return new Uint8Array();
  return readBoundedStream(response.body, maxBytes);
}

function finiteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

/** Validate only the response fields this proxy exposes; bound every attacker-controlled fanout. */
export function validSttResponse(value: unknown): value is {
  text?: string;
  segments?: UpstreamSegment[];
} {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const data = value as Record<string, unknown>;
  if (data.text !== undefined
      && (typeof data.text !== "string" || data.text.length > MAX_TRANSCRIPT_CHARS)) return false;
  if (data.segments === undefined) return true;
  if (!Array.isArray(data.segments) || data.segments.length > MAX_SEGMENTS) return false;
  let wordCount = 0;
  for (const rawSegment of data.segments) {
    if (!rawSegment || typeof rawSegment !== "object" || Array.isArray(rawSegment)) return false;
    const segment = rawSegment as Record<string, unknown>;
    if (segment.text !== undefined
        && (typeof segment.text !== "string" || segment.text.length > MAX_TRANSCRIPT_CHARS)) return false;
    if (segment.words === undefined) continue;
    if (!Array.isArray(segment.words)) return false;
    wordCount += segment.words.length;
    if (wordCount > MAX_WORDS) return false;
    for (const rawWord of segment.words) {
      if (!rawWord || typeof rawWord !== "object" || Array.isArray(rawWord)) return false;
      const word = rawWord as Record<string, unknown>;
      if (typeof word.word !== "string" || word.word.length > MAX_WORD_CHARS) return false;
      if (word.start !== undefined && !finiteNumber(word.start)) return false;
      if (word.end !== undefined && !finiteNumber(word.end)) return false;
    }
  }
  return true;
}
