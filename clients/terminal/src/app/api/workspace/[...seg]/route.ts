/** Read proxy for the workspace knowledge graph → agent-api /api/workspace/* (host stays server-side). */
import type { NextRequest } from "next/server";
import { resolveApiKey } from "../../proxyAuth";
import { credentialedFetch } from "../../credentialedFetch";
import { readBoundedBytes } from "../../boundedBody";
import { MAX_WORKSPACE_REQUEST_BYTES, MAX_WORKSPACE_RESPONSE_BYTES } from "../../proxyLimits";
import { meetingsOnly } from "../../../mode";

export const dynamic = "force-dynamic";

/** Meetings-only mode: the workspace KG is an agent surface — refused at the edge (404). */
function refusedResponse(): Response | null {
  if (!meetingsOnly()) return null;
  return new Response(JSON.stringify({ error: "not_found", detail: "agent endpoints are disabled in meetings mode" }), { status: 404, headers: { "Content-Type": "application/json" } });
}

function unauthorizedResponse(): Response {
  return new Response(JSON.stringify({ error: "unauthorized" }), {
    status: 401,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
  });
}

// One authenticated edge: workspace KG reads go through the gateway (which injects X-User-Id), not agent-api directly.
const GATEWAY_URL = (process.env.GATEWAY_URL || "http://127.0.0.1:18056").replace(/\/$/, "");

function jsonError(error: string, status: number, upstreamStatus?: number): Response {
  return new Response(JSON.stringify({ error, ...(upstreamStatus === undefined ? {} : { status: upstreamStatus }) }), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
  });
}

/** Fetch/Response BodyInit requires an owned ArrayBuffer under the current DOM typings; a generic
 * Uint8Array may be backed by SharedArrayBuffer. Copy once at the proxy boundary to make ownership
 * and the transmitted byte range exact. */
function ownedArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy.buffer;
}

async function workspaceResponse(upstream: Response): Promise<Response> {
  if (upstream.status === 204 || upstream.status === 205 || upstream.status === 304) {
    await upstream.body?.cancel("null-body status").catch(() => undefined);
    return new Response(null, { status: upstream.status, headers: { "Cache-Control": "no-store" } });
  }
  if (!upstream.ok) {
    await upstream.body?.cancel("untrusted upstream error").catch(() => undefined);
    return jsonError("upstream_error", upstream.status, upstream.status);
  }
  const body = await readBoundedBytes(upstream, MAX_WORKSPACE_RESPONSE_BYTES);
  if (body === null) return jsonError("upstream_response_too_large", 502);
  return new Response(ownedArrayBuffer(body), {
    status: upstream.status,
    headers: {
      "Content-Type": upstream.headers.get("Content-Type") || "application/json",
      "Cache-Control": "no-store",
    },
  });
}

export async function GET(req: NextRequest, ctx: { params: Promise<{ seg: string[] }> }) {
  const refused = refusedResponse();
  if (refused) return refused;
  const apiKey = await resolveApiKey();
  if (!apiKey) return unauthorizedResponse();
  const { seg } = await ctx.params;
  const path = seg.map((part) => encodeURIComponent(part)).join("/");
  try {
    const upstream = await credentialedFetch(`${GATEWAY_URL}/agent/workspace/${path}${req.nextUrl.search}`, {
      headers: { "X-API-Key": apiKey },
      signal: req.signal,
    });
    return workspaceResponse(upstream);
  } catch {
    console.error("[terminal-api] workspace read proxy failed");
    return jsonError("upstream_unavailable", 502);
  }
}

export async function POST(req: NextRequest, ctx: { params: Promise<{ seg: string[] }> }) {
  const refused = refusedResponse();
  if (refused) return refused;
  const apiKey = await resolveApiKey();
  if (!apiKey) return unauthorizedResponse();
  const { seg } = await ctx.params;
  const body = await readBoundedBytes(req, MAX_WORKSPACE_REQUEST_BYTES);
  if (body === null) return jsonError("request_too_large", 413);
  const path = seg.map((part) => encodeURIComponent(part)).join("/");
  try {
    const upstream = await credentialedFetch(`${GATEWAY_URL}/agent/workspace/${path}${req.nextUrl.search}`, {
      method: "POST",
      body: ownedArrayBuffer(body),
      headers: {
        "Content-Type": req.headers.get("Content-Type") ?? "",
        "X-API-Key": apiKey,
      },
      signal: req.signal,
    });
    return workspaceResponse(upstream);
  } catch {
    console.error("[terminal-api] workspace write proxy failed");
    return jsonError("upstream_unavailable", 502);
  }
}
