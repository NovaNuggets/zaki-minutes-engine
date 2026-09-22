/** POST /api/stt[?prompt=…] — speech-to-text proxy for composer dictation.
 *
 *  Body: audio/wav (16 kHz mono PCM from ui-kit/micDictation). Forwards to the same
 *  effective transcription service the meeting pipeline uses (atomic user/platform Settings,
 *  then the atomic `TRANSCRIPTION_SERVICE_URL/TOKEN` env bundle), at the OpenAI-compatible
 *  /v1/audio/transcriptions path — see @vexa/transcribe-whisper's
 *  TranscriptionClient for the canonical contract). `prompt` carries the already-
 *  confirmed text for context continuity (streaming re-submission, exactly like the
 *  meeting pipeline). Returns `{ text, words }` — word timestamps drive the client's
 *  LocalAgreement confirm/trim. The bearer token stays server-side.
 */
import { NextResponse } from "next/server";
import { getUserBotContext } from "../auth/adminApi";
import { resolveValidatedPrincipal } from "../proxyAuth";
import { credentialedFetch } from "../credentialedFetch";
import {
  MAX_STT_REQUEST_BYTES,
  MAX_STT_RESPONSE_BYTES,
  readBoundedBody,
  readBoundedResponse,
  validSttResponse,
} from "./sttSafety";

export const runtime = "nodejs";

const NO_STORE = { "Cache-Control": "no-store" } as const;

function jsonResponse(body: unknown, status = 200): NextResponse {
  return NextResponse.json(body, { status, headers: NO_STORE });
}

/** Match the meeting bot's canonical endpoint completion: root, `/v1`, `/v1/audio`, and the
 * complete path all resolve to one endpoint without doubling a caller-supplied suffix. */
function transcriptionEndpoint(serviceUrl: string): string {
  const base = serviceUrl.trim().replace(/\/+$/, "");
  if (base.endsWith("/v1/audio/transcriptions")) return base;
  if (base.endsWith("/v1/audio")) return `${base}/transcriptions`;
  if (base.endsWith("/v1")) return `${base}/audio/transcriptions`;
  return `${base}/v1/audio/transcriptions`;
}

export async function POST(req: Request): Promise<NextResponse> {
  // Auth gate: this forwards to the shared transcription service with a server-side
  // bearer token. Without a check it's an open, credentialed Whisper proxy (cost/abuse
  // vector) — require the same per-user key every other proxy route resolves.
  const principal = await resolveValidatedPrincipal();
  if (!principal) {
    return jsonResponse({ error: "Unauthorized" }, 401);
  }

  // Byte and identity gates reduce accidental/casual spend. Per-user STT quota/rate accounting is
  // still a separate launch follow-up; this route does not claim to be a billing quota boundary.
  const wav = await readBoundedBody(req, MAX_STT_REQUEST_BYTES);
  if (wav === null) return jsonResponse({ error: "Recording too long" }, 413);
  if (wav.byteLength < 100) return jsonResponse({ error: "Empty recording" }, 400);

  const context = await getUserBotContext(principal.userId);
  if (!context.ok || !context.data) {
    return jsonResponse({ error: "Transcription configuration is unavailable" }, 503);
  }
  if (context.data.transcription?.blocked) {
    return jsonResponse({ error: "Personal transcription configuration is blocked" }, 503);
  }
  const settingsUrl = (context.data.transcription?.url ?? "").trim();
  // A Settings URL selects its complete credential tier. Only an absent Settings URL permits the
  // deployment env bundle; never pair a user/platform URL with TRANSCRIPTION_SERVICE_TOKEN.
  const base = settingsUrl || (process.env.TRANSCRIPTION_SERVICE_URL ?? "").trim();
  const token = settingsUrl
    ? (context.data.transcription?.token ?? "").trim()
    : (process.env.TRANSCRIPTION_SERVICE_TOKEN ?? "").trim();
  if (!base) {
    return jsonResponse({ error: "Transcription is not configured" }, 503);
  }

  const prompt = new URL(req.url).searchParams.get("prompt") ?? "";

  const form = new FormData();
  form.append("file", new Blob([wav], { type: "audio/wav" }), "dictation.wav");
  form.append("model", "whisper-1");
  form.append("response_format", "verbose_json");
  form.append("timestamp_granularities", "word");
  if (prompt) form.append("prompt", prompt.slice(0, 800));

  const headers: Record<string, string> = {};
  if (token) headers.Authorization = `Bearer ${token}`;

  try {
    const r = await credentialedFetch(transcriptionEndpoint(base), {
      method: "POST",
      headers,
      body: form,
      signal: AbortSignal.timeout(30000),
    });
    if (!r.ok) {
      // Upstream text is untrusted and may echo Authorization; keep secrets server-side.
      await r.body?.cancel("untrusted transcription error").catch(() => undefined);
      return jsonResponse({ error: `Transcription failed (${r.status})` }, 502);
    }
    const responseBody = await readBoundedResponse(r, MAX_STT_RESPONSE_BYTES);
    if (responseBody === null) {
      return jsonResponse({ error: "Transcription returned an oversized response" }, 502);
    }
    let data: unknown;
    try {
      data = JSON.parse(new TextDecoder().decode(responseBody));
    } catch {
      return jsonResponse({ error: "Transcription returned invalid JSON" }, 502);
    }
    if (!validSttResponse(data)) {
      return jsonResponse({ error: "Transcription returned an invalid response" }, 502);
    }
    const words = (data.segments ?? []).flatMap((s) => s.words ?? [])
      .filter((w) => typeof w.word === "string")
      .map((w) => ({ word: w.word as string, start: w.start ?? 0, end: w.end ?? 0 }));
    return jsonResponse({ text: (data.text ?? "").trim(), words });
  } catch (err) {
    const timeout = err instanceof Error && err.name === "TimeoutError";
    return jsonResponse({ error: timeout ? "Transcription timed out" : "Transcription unreachable" }, 502);
  }
}
