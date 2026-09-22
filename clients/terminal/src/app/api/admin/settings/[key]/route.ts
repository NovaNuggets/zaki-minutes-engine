/** Admin platform-settings editor — the Settings surface's GLOBAL defaults (models /
 *  transcription), proxied server-side to admin-api's internal-tier `/internal/settings/{key}`.
 *  Same gate + hiding as the infra panel: a VERIFIED allowlisted admin (../gate.ts) or a plain
 *  404, and the X-Internal-Secret never reaches the browser. Unlike /api/admin/overview this
 *  route WRITES (PUT). The internal admin-api response carries cleartext for trusted service
 *  consumers, so this browser boundary masks write-only secrets on every read-back. */
import { NextRequest, NextResponse } from "next/server";
import { requireAdmin } from "../../gate";
import { credentialedFetch } from "../../../credentialedFetch";
import { readBoundedText } from "../../../boundedBody";
import { MAX_SETTINGS_REQUEST_BYTES, MAX_SETTINGS_RESPONSE_BYTES } from "../../../proxyLimits";

export const dynamic = "force-dynamic";

const SECRET_FIELD: Record<string, string> = {
  models: "api_key",
  transcription: "token",
};
const ALLOWED_FIELDS: Record<string, Set<string>> = {
  models: new Set(["mode", "model", "meeting_model", "base_url", "api_key"]),
  transcription: new Set(["url", "token"]),
  setup: new Set(["models", "transcription", "completed"]),
};
function maskSecret(secret: string): string {
  return "********" + (secret.length > 8 ? secret.slice(-4) : "");
}

function valuesAreValid(key: string, value: Record<string, unknown>): boolean {
  if (Object.keys(value).some((field) => !ALLOWED_FIELDS[key]?.has(field))) return false;
  if (Object.values(value).some((fieldValue) => typeof fieldValue !== "string")) return false;
  if (key !== "setup") return true;
  return Object.entries(value).every(([field, fieldValue]) => {
    if (field === "completed") return fieldValue === "" || fieldValue === "true";
    return fieldValue === "" || fieldValue === "done" || fieldValue === "skipped";
  });
}

/** Redact the internal settings shape before it crosses into browser JavaScript. Requests remain
 * partial: omitted fields stay omitted, while an explicit empty string still clears a secret. */
function browserSafeBody(key: string, raw: string): string | null {
  try {
    const parsed = JSON.parse(raw) as { key?: unknown; value?: Record<string, unknown> } | null;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    if (Object.keys(parsed).some((field) => field !== "key" && field !== "value")) return null;
    if (parsed.key !== key) return null;
    if (!parsed.value || typeof parsed.value !== "object" || Array.isArray(parsed.value)) return null;
    if (!valuesAreValid(key, parsed.value)) return null;
    const value = { ...parsed.value };
    const secretField = SECRET_FIELD[key];
    const secret = value[secretField];
    if (secret !== undefined && secret !== null && typeof secret !== "string") return null;
    if (typeof secret === "string" && secret) value[secretField] = maskSecret(secret);
    return JSON.stringify({ key, value });
  } catch {
    return null;
  }
}

function validatedUpdateBody(key: string, raw: string): string | null {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    const update = parsed as Record<string, unknown>;
    if (!valuesAreValid(key, update)) return null;
    const secretField = SECRET_FIELD[key];
    if (secretField && String(update[secretField] ?? "").startsWith("********")) return null;
    return JSON.stringify(update);
  } catch {
    return null;
  }
}

async function proxy(req: NextRequest, key: string, method: "GET" | "PUT") {
  // This edge is deliberately narrower than admin-api's generic settings endpoint. Unknown keys
  // never reach the privileged internal hop.
  if (!ALLOWED_FIELDS[key]) return new NextResponse(null, { status: 404 });
  const admin = await requireAdmin();
  if (!admin) return new NextResponse(null, { status: 404 });

  const adminApiUrl = (process.env.VEXA_ADMIN_API_URL || "").replace(/\/$/, "");
  const secret = process.env.VEXA_INTERNAL_API_SECRET || "";
  if (!adminApiUrl || !secret) {
    return NextResponse.json(
      { error: "Admin API is not configured (VEXA_ADMIN_API_URL / VEXA_INTERNAL_API_SECRET)" },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }
  try {
    let requestBody: string | undefined;
    if (method === "PUT") {
      const raw = await readBoundedText(req, MAX_SETTINGS_REQUEST_BYTES);
      if (raw === null) {
        return NextResponse.json(
          { error: "settings request is too large" },
          { status: 413, headers: { "Cache-Control": "no-store" } },
        );
      }
      requestBody = validatedUpdateBody(key, raw) ?? undefined;
      if (requestBody === undefined) {
        return NextResponse.json(
          { error: "invalid settings update" },
          { status: 400, headers: { "Cache-Control": "no-store" } },
        );
      }
    }
    const res = await credentialedFetch(`${adminApiUrl}/internal/settings/${encodeURIComponent(key)}`, {
      method,
      headers: { "X-Internal-Secret": secret, "Content-Type": "application/json" },
      body: requestBody,
      cache: "no-store",
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) {
      await res.body?.cancel("untrusted admin-api error").catch(() => undefined);
      return NextResponse.json(
        { error: "admin-api settings request failed", status: res.status },
        { status: res.status, headers: { "Cache-Control": "no-store" } },
      );
    }
    const rawResponse = await readBoundedText(res, MAX_SETTINGS_RESPONSE_BYTES);
    const body = rawResponse === null ? null : browserSafeBody(key, rawResponse);
    if (body === null) {
      return NextResponse.json(
        { error: "admin-api returned an invalid settings response" },
        { status: 502, headers: { "Cache-Control": "no-store" } },
      );
    }
    return new NextResponse(body, {
      status: res.status,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    });
  } catch {
    return NextResponse.json(
      { error: "admin-api unreachable" },
      { status: 502, headers: { "Cache-Control": "no-store" } },
    );
  }
}

export async function GET(req: NextRequest, ctx: { params: Promise<{ key: string }> }) {
  return proxy(req, (await ctx.params).key, "GET");
}

export async function PUT(req: NextRequest, ctx: { params: Promise<{ key: string }> }) {
  return proxy(req, (await ctx.params).key, "PUT");
}
