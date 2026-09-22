/** Server-only admin-api client for the terminal's own auth.
 *
 *  Mirrors the dashboard's pattern (clients/dashboard/src/lib/vexa-admin-api.ts) WITHOUT importing it
 *  — the dashboard is being retired. The terminal owns a tiny slice: find-or-create a user by email and
 *  mint an APIToken. identity.v1 carries bot/tx/browser; the Agent scope is requested only after
 *  the admin capability route explicitly advertises identity.v2. All calls carry X-Admin-API-Key and are never cached
 *  (a cached 404 would make find-or-create fabricate duplicate users).
 */

import { credentialedFetch } from "../credentialedFetch";
import { readBoundedText } from "../boundedBody";

export const MAX_ADMIN_REQUEST_BYTES = 64 * 1024;
export const MAX_ADMIN_RESPONSE_BYTES = 256 * 1024;

function requestBodyIsBounded(body: BodyInit | null | undefined): boolean {
  if (body === null || body === undefined) return true;
  return typeof body === "string" && new TextEncoder().encode(body).byteLength <= MAX_ADMIN_REQUEST_BYTES;
}

async function readAdminJson<T>(res: Response): Promise<{ ok: true; data: T } | { ok: false }> {
  const raw = await readBoundedText(res, MAX_ADMIN_RESPONSE_BYTES);
  if (raw === null) return { ok: false };
  try {
    return { ok: true, data: JSON.parse(raw) as T };
  } catch {
    return { ok: false };
  }
}

export const AUTH_COOKIE = process.env.VEXA_AUTH_COOKIE_NAME || "vexa-token";
export const USER_INFO_COOKIE = process.env.VEXA_USER_INFO_COOKIE_NAME || "vexa-user-info";

export interface AdminUser {
  id: string | number;
  email: string;
  name?: string | null;
  max_concurrent_bots?: number;
  created_at?: string;
}

export interface AdminResult<T> {
  ok: boolean;
  status: number;
  data?: T;
  notFound?: boolean;
  error?: string;
}

function adminConfig(): { url: string; key: string } | null {
  const url = (process.env.VEXA_ADMIN_API_URL || "").replace(/\/$/, "");
  const key = process.env.VEXA_ADMIN_API_KEY || "";
  if (!url || !key || key === "your_admin_api_key_here") return null;
  return { url, key };
}

async function adminRequest<T>(path: string, init: RequestInit = {}, timeout = 15000): Promise<AdminResult<T>> {
  const cfg = adminConfig();
  if (!cfg) return { ok: false, status: 503, error: "Admin API is not configured (VEXA_ADMIN_API_URL / VEXA_ADMIN_API_KEY)" };
  if (!requestBodyIsBounded(init.body)) {
    return { ok: false, status: 413, error: "Admin API request is too large" };
  }

  try {
    const res = await credentialedFetch(`${cfg.url}${path}`, {
      ...init,
      headers: { "Content-Type": "application/json", "X-Admin-API-Key": cfg.key, ...init.headers },
      cache: "no-store",
      signal: AbortSignal.timeout(timeout),
    });

    if (res.status === 404) {
      await res.body?.cancel("untrusted admin-api error").catch(() => undefined);
      return { ok: false, status: 404, notFound: true };
    }
    if (!res.ok) {
      await res.body?.cancel("untrusted admin-api error").catch(() => undefined);
      return { ok: false, status: res.status, error: `admin-api returned ${res.status}` };
    }
    if (res.status === 204) return { ok: true, status: 204 };
    const decoded = await readAdminJson<T>(res);
    if (!decoded.ok) return { ok: false, status: 502, error: "admin-api returned an invalid response" };
    return { ok: true, status: res.status, data: decoded.data };
  } catch (err) {
    const e = err as Error;
    return { ok: false, status: 0, error: e.name === "TimeoutError" ? "admin-api request timed out" : "admin-api unavailable" };
  }
}

export function findUserByEmail(email: string): Promise<AdminResult<AdminUser>> {
  return adminRequest<AdminUser>(`/admin/users/email/${encodeURIComponent(email)}`, { method: "GET" });
}

export function createUser(email: string): Promise<AdminResult<AdminUser>> {
  return adminRequest<AdminUser>(`/admin/users`, { method: "POST", body: JSON.stringify({ email }) });
}

export const IDENTITY_V1 = "identity.v1" as const;
export const IDENTITY_V2 = "identity.v2" as const;
export const IDENTITY_V1_SCOPES = ["bot", "tx", "browser"] as const;
export const IDENTITY_V2_SCOPES = [...IDENTITY_V1_SCOPES, "agent"] as const;
export type IdentityContractVersion = typeof IDENTITY_V1 | typeof IDENTITY_V2;

export interface IdentityTokenCapabilities {
  version: IdentityContractVersion;
  scopes: readonly string[];
}

/** Negotiate the identity token contract. A 404 is the one safe legacy signal: old servers are
 * v1. Any other failure or malformed 200 is fatal; after v2 is advertised callers never retry v1. */
export async function getIdentityTokenCapabilities(): Promise<AdminResult<IdentityTokenCapabilities>> {
  const response = await adminRequest<unknown>("/admin/capabilities", { method: "GET" });
  if (!response.ok) {
    if (response.notFound) {
      return { ok: true, status: 200, data: { version: IDENTITY_V1, scopes: IDENTITY_V1_SCOPES } };
    }
    return response as AdminResult<IdentityTokenCapabilities>;
  }
  const raw = response.data;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return { ok: false, status: 502, error: "admin-api returned invalid identity capabilities" };
  }
  const contracts = (raw as Record<string, unknown>).contracts;
  const identity = contracts && typeof contracts === "object" && !Array.isArray(contracts)
    ? (contracts as Record<string, unknown>).identity : undefined;
  const versions = identity && typeof identity === "object" && !Array.isArray(identity)
    ? (identity as Record<string, unknown>).versions : undefined;
  const preferred = identity && typeof identity === "object" && !Array.isArray(identity)
    ? (identity as Record<string, unknown>).preferred : undefined;
  if (!Array.isArray(versions)
      || versions.length === 0
      || versions.length > 16
      || versions.some((item) => typeof item !== "string" || item.length > 64)
      || typeof preferred !== "string") {
    return { ok: false, status: 502, error: "admin-api returned invalid identity capabilities" };
  }
  if (versions.includes(IDENTITY_V2) && preferred === IDENTITY_V2) {
    return { ok: true, status: response.status, data: { version: IDENTITY_V2, scopes: IDENTITY_V2_SCOPES } };
  }
  if (versions.includes(IDENTITY_V1) && preferred === IDENTITY_V1) {
    return { ok: true, status: response.status, data: { version: IDENTITY_V1, scopes: IDENTITY_V1_SCOPES } };
  }
  return { ok: false, status: 502, error: "admin-api advertised no supported identity contract" };
}

export function createUserToken(
  userId: string | number,
  capabilities: IdentityTokenCapabilities,
): Promise<AdminResult<{ token: string }>> {
  const query = new URLSearchParams({ scopes: capabilities.scopes.join(",") });
  if (capabilities.version === IDENTITY_V2) query.set("contract_version", IDENTITY_V2);
  return adminRequest<{ token: string }>(
    `/admin/users/${encodeURIComponent(String(userId))}/tokens?${query.toString()}`,
    { method: "POST" },
  );
}

// ── verified identity — admin-api's internal oracle (`POST /internal/validate`, the same
//    X-Internal-Secret edge the gateway uses). The `vexa-token` auth cookie is the ONLY input; the
//    returned {user_id, email} is the ONLY identity this server trusts. The `vexa-user-info` cookie
//    is display-only: httpOnly stops JS reads, not a hand-crafted Cookie header, so nothing
//    security-relevant may ever be derived from it.

export type ValidatedUser =
  | { ok: true; userId: string | number; email: string; isAdmin: boolean }
  | { ok: false; status: number; error: string };

function isValidUserId(value: unknown): value is string | number {
  return (typeof value === "string" && value.length > 0 && value.length <= 128)
    || (typeof value === "number" && Number.isSafeInteger(value) && value >= 0);
}

function isValidEmail(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 320;
}

export interface BotContext {
  transcription?: {
    url?: string;
    token?: string;
    blocked?: boolean;
    config_status?: "valid" | "blocked" | "incomplete";
    validation_error?: string;
  };
}

export async function validateAuthToken(token: string): Promise<ValidatedUser> {
  const url = (process.env.VEXA_ADMIN_API_URL || "").replace(/\/$/, "");
  const secret = process.env.VEXA_INTERNAL_API_SECRET || "";
  if (!url || !secret) {
    // Fail closed — an unconfigured oracle must never fall back to trusting client-sendable data.
    return { ok: false, status: 503, error: "Auth validation is not configured (VEXA_ADMIN_API_URL / VEXA_INTERNAL_API_SECRET)" };
  }
  const requestBody = JSON.stringify({ token });
  if (!requestBodyIsBounded(requestBody)) {
    return { ok: false, status: 413, error: "Auth validation request is too large" };
  }

  try {
    const res = await credentialedFetch(`${url}/internal/validate`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Internal-Secret": secret },
      body: requestBody,
      cache: "no-store",
      signal: AbortSignal.timeout(8000),
    });
    if (res.status === 401) {
      await res.body?.cancel("untrusted auth error").catch(() => undefined);
      return { ok: false, status: 401, error: "Not authenticated" };
    }
    if (!res.ok) {
      await res.body?.cancel("untrusted auth error").catch(() => undefined);
      return { ok: false, status: 503, error: `Token validation failed (admin-api returned ${res.status})` };
    }
    const decoded = await readAdminJson<{ user_id?: string | number; email?: string; is_admin?: boolean }>(res);
    if (!decoded.ok) return { ok: false, status: 502, error: "Token validation returned an invalid response" };
    const data = decoded.data;
    const validAdmin = data.is_admin === undefined || typeof data.is_admin === "boolean";
    if (!isValidUserId(data.user_id) || !isValidEmail(data.email) || !validAdmin) {
      return { ok: false, status: 502, error: "Token validation returned no identity" };
    }
    return { ok: true, userId: data.user_id, email: data.email, isAdmin: data.is_admin === true };
  } catch (err) {
    const e = err as Error;
    return { ok: false, status: 503, error: e.name === "TimeoutError" ? "Token validation timed out" : "Token validation unavailable" };
  }
}

// ── first-run bootstrap admin — a fresh instance has NO admin; the first successful sign-in
//    claims the role (admin-api serializes concurrent claims). A configured VEXA_ADMIN_EMAILS
//    allowlist means the instance ALREADY has admins → the claim machinery stays off entirely,
//    which also keeps existing deployments (allowlist-run) from handing admin to the next login.

function allowlistConfigured(): boolean {
  return (process.env.VEXA_ADMIN_EMAILS || "").split(",").some((e) => e.trim());
}

async function internalRequest<T>(path: string, init: RequestInit = {}): Promise<AdminResult<T>> {
  const url = (process.env.VEXA_ADMIN_API_URL || "").replace(/\/$/, "");
  const secret = process.env.VEXA_INTERNAL_API_SECRET || "";
  if (!url || !secret) {
    return { ok: false, status: 503, error: "Admin API internal edge is not configured (VEXA_ADMIN_API_URL / VEXA_INTERNAL_API_SECRET)" };
  }
  if (!requestBodyIsBounded(init.body)) {
    return { ok: false, status: 413, error: "Admin API request is too large" };
  }
  try {
    const res = await credentialedFetch(`${url}${path}`, {
      ...init,
      headers: { "Content-Type": "application/json", "X-Internal-Secret": secret, ...init.headers },
      cache: "no-store",
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) {
      await res.body?.cancel("untrusted admin-api error").catch(() => undefined);
      return { ok: false, status: res.status, error: `admin-api returned ${res.status}` };
    }
    const decoded = await readAdminJson<T>(res);
    if (!decoded.ok) return { ok: false, status: 502, error: "admin-api returned an invalid response" };
    return { ok: true, status: res.status, data: decoded.data };
  } catch (err) {
    const e = err as Error;
    return { ok: false, status: 0, error: e.name === "TimeoutError" ? "admin-api request timed out" : "admin-api unavailable" };
  }
}

/** Effective meeting/STT context for one already-validated user. The identity service resolves
 * the atomic user tier or platform tier; callers apply env only when this returns no URL. */
export async function getUserBotContext(userId: string | number): Promise<AdminResult<BotContext>> {
  const result = await internalRequest<unknown>(
    `/internal/users/${encodeURIComponent(String(userId))}/bot-context`,
    { method: "GET" },
  );
  if (!result.ok) {
    return {
      ok: false,
      status: result.status,
      error: result.error,
      notFound: result.notFound,
    };
  }
  const raw = result.data;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return { ok: false, status: 502, error: "admin-api returned an invalid bot context" };
  }
  const transcription = (raw as Record<string, unknown>).transcription;
  if (transcription === undefined) return { ok: true, status: result.status, data: {} };
  if (!transcription || typeof transcription !== "object" || Array.isArray(transcription)) {
    return { ok: false, status: 502, error: "admin-api returned an invalid bot context" };
  }
  const value = transcription as Record<string, unknown>;
  const validString = (field: unknown, max: number) => field === undefined
    || (typeof field === "string" && field.length <= max);
  if (!validString(value.url, 2048)
      || !validString(value.token, 8192)
      || !validString(value.validation_error, 512)
      || (value.blocked !== undefined && typeof value.blocked !== "boolean")
      || (value.config_status !== undefined
        && !["valid", "blocked", "incomplete"].includes(String(value.config_status)))) {
    return { ok: false, status: 502, error: "admin-api returned an invalid bot context" };
  }
  return {
    ok: true,
    status: result.status,
    data: { transcription: value as BotContext["transcription"] },
  };
}

/** Does this instance have an admin yet? An allowlist counts as "yes" (those emails ARE admins).
 *  FAIL-SAFE towards true: if the probe can't answer, the login surface shows plain sign-in
 *  rather than dangling a claim screen that can't succeed. */
export async function instanceHasAdmin(): Promise<boolean> {
  if (allowlistConfigured()) return true;
  const res = await internalRequest<{ admin_exists?: boolean }>("/internal/instance", { method: "GET" });
  if (!res.ok || !res.data) return true;
  return res.data.admin_exists === true;
}

/** Claim the admin role for this user IF the instance has none — the "first sign-in = admin"
 *  step, called on every successful login (admin-api makes it a no-op once an admin exists).
 *  BEST-EFFORT: a failure must never block sign-in; the claim screen simply reappears. */
async function bootstrapAdminClaim(userId: string | number): Promise<void> {
  if (allowlistConfigured()) return; // allowlist-run instance → role claims stay off
  const res = await internalRequest<{ claimed?: boolean }>("/internal/bootstrap-admin", {
    method: "POST",
    body: JSON.stringify({ user_id: userId }),
  });
  if (res.ok && res.data?.claimed) {
    console.info("[terminal-auth] first administrator role claimed");
  } else if (!res.ok) {
    console.warn("[terminal-auth] bootstrap-admin claim failed (sign-in continues)");
  }
}

// ── token self-serve (the /api/tokens routes) — admin-tier calls, ALWAYS scoped to the logged-in
//    user's own user_id (resolved server-side from the auth cookies; never taken from the client).

/** A token as admin-api lists it — metadata only, never the secret value. */
export interface AdminTokenInfo {
  id: number;
  user_id: number;
  scopes: string[];
  name?: string | null;
  created_at?: string | null;
  last_used_at?: string | null;
  expires_at?: string | null;
}

/** The mint response — the ONLY place the token value ever crosses. */
export interface AdminMintedToken extends AdminTokenInfo {
  token: string;
}

export function listUserTokens(userId: string | number): Promise<AdminResult<AdminTokenInfo[]>> {
  return adminRequest<AdminTokenInfo[]>(
    `/admin/users/${encodeURIComponent(String(userId))}/tokens`,
    { method: "GET" },
  );
}

export function mintUserToken(
  userId: string | number,
  opts: { scopes: string[]; name?: string; expiresIn?: number; contractVersion: IdentityContractVersion },
): Promise<AdminResult<AdminMintedToken>> {
  const q = new URLSearchParams({ scopes: opts.scopes.join(",") });
  if (opts.contractVersion === IDENTITY_V2) q.set("contract_version", IDENTITY_V2);
  if (opts.name) q.set("name", opts.name);
  if (opts.expiresIn && opts.expiresIn > 0) q.set("expires_in", String(opts.expiresIn));
  return adminRequest<AdminMintedToken>(
    `/admin/users/${encodeURIComponent(String(userId))}/tokens?${q.toString()}`,
    { method: "POST" },
  );
}

export function revokeToken(tokenId: string | number): Promise<AdminResult<void>> {
  return adminRequest<void>(`/admin/tokens/${encodeURIComponent(String(tokenId))}`, { method: "DELETE" });
}

// One authenticated edge: provisioning goes through the gateway (which resolves the api-key → user_id and
// injects X-User-Id), never agent-api directly. Mirrors the workspace proxy route's GATEWAY_URL default.
const GATEWAY_URL = (process.env.GATEWAY_URL || "http://127.0.0.1:18056").replace(/\/$/, "");

/** EAGERLY provision the user's agent workspace tiers (Personal baseline + private `_system`) so they
 *  exist from account creation instead of being lazily seeded on the first chat. BEST-EFFORT: the call is
 *  idempotent server-side and the lazy first-dispatch path is a full fallback, so any failure here (agent
 *  down, slow, misconfig) is logged and swallowed — it must NEVER block sign-in. Authenticates with the
 *  freshly minted api-key over the gateway's `/agent/workspace/*` edge. */
async function provisionUserWorkspace(token: string): Promise<void> {
  try {
    const res = await credentialedFetch(`${GATEWAY_URL}/agent/workspace/init`, {
      method: "POST",
      headers: { "X-API-Key": token, "Content-Type": "application/json" },
      cache: "no-store",
      signal: AbortSignal.timeout(12000),
    });
    await res.body?.cancel("workspace initialization response is not consumed").catch(() => undefined);
    if (!res.ok) {
      console.warn(`[terminal-auth] eager workspace provisioning returned ${res.status} (lazy seeding will cover it)`);
    }
  } catch {
    console.warn("[terminal-auth] eager workspace provisioning failed (lazy seeding will cover it)");
  }
}

/** Find the user by email, creating them if they don't exist, then mint an APIToken.
 *  Returns the user + token, or an error with an HTTP-ish status for the caller to surface. */
export async function findOrCreateUserToken(
  email: string,
  options: { bootstrapAdmin?: boolean } = {},
): Promise<{ ok: true; user: AdminUser; token: string } | { ok: false; status: number; error: string }> {
  const found = await findUserByEmail(email);

  let user: AdminUser | undefined;
  let justCreated = false;
  if (found.ok && found.data) {
    user = found.data;
  } else if (found.notFound) {
    const created = await createUser(email);
    if (!created.ok || !created.data) {
      return { ok: false, status: created.status || 500, error: created.error || "Failed to create user" };
    }
    user = created.data;
    justCreated = true;
  } else {
    return { ok: false, status: found.status || 503, error: found.error || "Failed to look up user" };
  }

  const capabilities = await getIdentityTokenCapabilities();
  if (!capabilities.ok || !capabilities.data) {
    return {
      ok: false,
      status: capabilities.status || 503,
      error: capabilities.error || "Failed to negotiate identity contract",
    };
  }
  const minted = await createUserToken(user.id, capabilities.data);
  if (!minted.ok || !minted.data?.token) {
    return { ok: false, status: minted.status || 500, error: minted.error || "Failed to mint API token" };
  }
  // First-run bootstrap: on a fresh instance the FIRST successful sign-in claims the admin role
  // (no-op everywhere else — admin exists, or an allowlist runs the instance). OAuth uses this
  // default; the local direct-login route explicitly disables the claim.
  if (options.bootstrapAdmin !== false) {
    await bootstrapAdminClaim(user.id);
  }
  // On genuine account creation ("account start"), eagerly provision the user's workspace tiers so the
  // Personal baseline + `_system` exist before their first chat. Best-effort (idempotent + lazy fallback).
  if (justCreated) {
    await provisionUserWorkspace(minted.data.token);
  }
  return { ok: true, user, token: minted.data.token };
}
