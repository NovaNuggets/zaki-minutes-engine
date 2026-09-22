/** Direct email login — no SMTP, no magic link. POST {email} → find-or-create the user at admin-api,
 *  mint an APIToken (scopes bot,tx,browser,agent), set the httpOnly `vexa-token` + `vexa-user-info` cookies.
 *
 *  Mirrors the dashboard's VEXA_ALLOW_DIRECT_LOGIN branch (without importing it). No email is ever sent.
 *  Must never be cached — a cached response would pin one identity for every subsequent login.
 */
import { NextResponse, type NextRequest } from "next/server";
import { cookies } from "next/headers";
import { AUTH_COOKIE, USER_INFO_COOKIE, findOrCreateUserToken } from "../adminApi";
import { readBoundedText } from "../../boundedBody";
import { MAX_AUTH_REQUEST_BYTES } from "../../proxyLimits";
import { directLoginEmailAllowed, directLoginEnabled } from "../../../../proxyAuthPolicy.mjs";

export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";

const NO_STORE = { "Cache-Control": "no-store, no-cache, must-revalidate" } as const;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function isSecureRequest(): boolean {
  return (
    (process.env.TERMINAL_URL || "").startsWith("https://") ||
    (process.env.NEXTAUTH_URL || "").startsWith("https://") ||
    false
  );
}

export async function POST(request: NextRequest) {
  // Default-off, local-only debug surface. Refuse before inspecting attacker-controlled input or
  // spending the admin credential; normal OAuth/session auth is a separate route and is unchanged.
  if (!directLoginEnabled(process.env)) {
    return NextResponse.json({ error: "not_found" }, { status: 404, headers: NO_STORE });
  }

  let body: unknown;
  try {
    const raw = await readBoundedText(request, MAX_AUTH_REQUEST_BYTES);
    if (raw === null) {
      return NextResponse.json({ error: "Request body is too large" }, { status: 413, headers: NO_STORE });
    }
    body = JSON.parse(raw);
  } catch {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400, headers: NO_STORE });
  }

  const email = body && typeof body === "object" && !Array.isArray(body)
    ? (body as { email?: unknown }).email
    : undefined;

  if (typeof email !== "string" || !email.trim()) {
    return NextResponse.json({ error: "Email is required" }, { status: 400, headers: NO_STORE });
  }
  const normalized = email.trim().toLowerCase();
  if (normalized.length > 320 || !EMAIL_RE.test(normalized)) {
    return NextResponse.json({ error: "Invalid email format" }, { status: 400, headers: NO_STORE });
  }
  if (!directLoginEmailAllowed(normalized, process.env)) {
    return NextResponse.json(
      { error: "Direct email login is not allowed for this account." },
      { status: 403, headers: NO_STORE },
    );
  }

  // A debug allowlisted identity may never become the first administrator. OAuth retains the normal
  // bootstrap path; this route explicitly suppresses it even on a fresh instance.
  const result = await findOrCreateUserToken(normalized, { bootstrapAdmin: false });
  if (!result.ok) {
    return NextResponse.json({ error: result.error }, { status: result.status || 500, headers: NO_STORE });
  }

  const { user, token } = result;
  const secure = isSecureRequest();
  const cookieStore = await cookies();
  const opts = { httpOnly: true, secure, sameSite: "lax" as const, maxAge: 60 * 60 * 24 * 30, path: "/" };
  cookieStore.set(AUTH_COOKIE, token, opts);
  cookieStore.set(USER_INFO_COOKIE, JSON.stringify({ email: user.email, name: user.name || user.email.split("@")[0] }), opts);

  return NextResponse.json(
    { success: true, user: { id: user.id, email: user.email, name: user.name ?? user.email } },
    { headers: NO_STORE },
  );
}
