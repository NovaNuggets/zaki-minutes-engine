/** The ONE proxy — a single catch-all that forwards every /api/* call to the right backend,
 *  keeping hosts + keys server-side. It replaces the ~9 near-identical thin route files.
 *
 *  Path-based routing (the architecture seam — two domains behind ONE authenticated edge, the gateway):
 *    • meetings · transcripts · bots  → the gateway ROOT paths (/meetings, /transcripts/{…}, /bots),
 *      where meeting-api is fronted.
 *    • everything else (chat · sessions · routines · workspace · models · …) → the gateway's /agent/*
 *      prefix, where agent-api is fronted.
 *  BOTH carry the per-user X-API-Key (login cookie, or an explicit local shared-key mode). The gateway
 *  resolves it → user and injects X-User-Id downstream, so agent-api derives `subject` from identity
 *  (the client never sends one — P20 scope). The terminal never reaches agent-api directly.
 *
 *  Carries through: the path after /api/, the query string, the request body, and the upstream
 *  status + JSON. SSE (/api/chat, /api/meeting/stream) and the workspace KG reader (/api/workspace/[...seg])
 *  stay as their own files — they need streaming / segment-specific shaping (all → the gateway).
 */
import type { NextRequest } from "next/server";
import { resolveApiKey } from "../proxyAuth";
import { credentialedFetch } from "../credentialedFetch";
import { readBoundedText } from "../boundedBody";
import { MAX_PROXY_ERROR_RESPONSE_BYTES, MAX_PROXY_REQUEST_BYTES, MAX_PROXY_RESPONSE_BYTES } from "../proxyLimits";
import { isManagedMinutesPath, MEETINGS_DOMAIN, refusedInMeetingsMode } from "../proxyMode";

export const dynamic = "force-dynamic";

const GATEWAY_URL = (process.env.GATEWAY_URL || "http://127.0.0.1:18056").replace(/\/$/, "");
// Two domains behind ONE authenticated edge (the gateway):
//   • meetings · transcripts · bots  → the gateway ROOT (/meetings, …) — meeting-api behind it.
//   • everything else (chat · sessions · routines · workspace · models · …) → the gateway's /agent/*
//     prefix — agent-api behind it.
// BOTH carry the per-user X-API-Key; the gateway resolves it → user and injects X-User-Id downstream,
// so the client never sends a `subject` (scope is server-derived — P20). agent-api is never reached directly.
// MEETINGS_DOMAIN (the meetings-vs-agent split) lives in ../proxyMode — shared with the meetings-only gate.

/** Resolve the upstream URL + headers for an already-authenticated browser request. */
function upstreamFor(path: string, search: string, apiKey: string): { url: string; headers: HeadersInit } {
  const base = MEETINGS_DOMAIN.test(path) ? `${GATEWAY_URL}/${path}` : `${GATEWAY_URL}/agent/${path}`;
  return { url: `${base}${search}`, headers: { "X-API-Key": apiKey } };
}

async function forward(req: NextRequest, params: Promise<{ path: string[] }>): Promise<Response> {
  const { path } = await params;
  const joined = path.join("/");
  if (isManagedMinutesPath(joined)) {
    return new Response(JSON.stringify({
      error: "managed_minutes_unavailable",
      detail: "Managed Minutes controls are available in the ZAKI Hub, not this reference Terminal.",
    }), {
      status: 404,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    });
  }
  // Meetings-only mode (NEXT_PUBLIC_TERMINAL_MODE=meetings): the agent branch is refused at the edge —
  // hiding the surfaces client-side is not enough, a hand-crafted request must not reach agent-api.
  if (refusedInMeetingsMode(joined)) {
    return new Response(JSON.stringify({ error: "not_found", detail: "agent endpoints are disabled in meetings mode" }), { status: 404, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });
  }
  const apiKey = await resolveApiKey();
  if (!apiKey) {
    return new Response(JSON.stringify({ error: "unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    });
  }
  const { url, headers } = upstreamFor(joined, req.nextUrl.search, apiKey);

  const init: RequestInit = { method: req.method, headers: { ...headers }, cache: "no-store" };
  if (req.method !== "GET" && req.method !== "DELETE") {
    const body = await readBoundedText(req, MAX_PROXY_REQUEST_BYTES);
    if (body === null) {
      return new Response(JSON.stringify({ error: "request_too_large" }), {
        status: 413,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
      });
    }
    if (body) {
      init.body = body;
      (init.headers as Record<string, string>)["Content-Type"] = "application/json";
    }
  }

  try {
    const upstream = await credentialedFetch(url, init);
    const contentType = upstream.headers.get("Content-Type") || "";
    if (upstream.ok && contentType.includes("text/event-stream")) {
      return new Response(upstream.body, {
        status: upstream.status,
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-store",
          "Connection": "keep-alive",
          "X-Accel-Buffering": "no",
        },
      });
    }

    // 204/205/304 are null-body statuses: new Response(body, …) throws for them (undici),
    // which would land in the catch below and turn a successful DELETE into a 502.
    if (upstream.status === 204 || upstream.status === 205 || upstream.status === 304) {
      return new Response(null, { status: upstream.status, headers: { "Cache-Control": "no-store" } });
    }

    if (!upstream.ok) {
      await readBoundedText(upstream, MAX_PROXY_ERROR_RESPONSE_BYTES);
      return new Response(JSON.stringify({ error: "upstream_error", status: upstream.status }), {
        status: upstream.status,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
      });
    }

    const body = await readBoundedText(upstream, MAX_PROXY_RESPONSE_BYTES);
    if (body === null) {
      return new Response(JSON.stringify({ error: "upstream_response_too_large" }), {
        status: 502,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
      });
    }
    return new Response(body, {
      status: upstream.status,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    });
  } catch {
    // upstream unreachable (gateway down / DNS). FAIL-LOUD (P18): return a real error body + 502 so the
    // client surfaces "backend unreachable" — never a silent empty {} that masquerades as "no data".
    return new Response(JSON.stringify({ error: "upstream_unreachable" }), { status: 502, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });
  }
}

type Ctx = { params: Promise<{ path: string[] }> };

export const GET = (req: NextRequest, ctx: Ctx) => forward(req, ctx.params);
export const POST = (req: NextRequest, ctx: Ctx) => forward(req, ctx.params);
export const PUT = (req: NextRequest, ctx: Ctx) => forward(req, ctx.params);
export const PATCH = (req: NextRequest, ctx: Ctx) => forward(req, ctx.params);
export const DELETE = (req: NextRequest, ctx: Ctx) => forward(req, ctx.params);
