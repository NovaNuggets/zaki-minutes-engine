/** The logged-in user's API tokens — GET lists, POST mints (the token value crosses ONCE, in the
 *  POST response, and is never retrievable again).
 *
 *  admin-api's token endpoints are ADMIN-tier, so these routes call it with the server's
 *  VEXA_ADMIN_API_KEY (the same way /api/auth/login does) and scope EVERY operation to the user_id
 *  resolved from the auth cookies (currentUser.ts) — a user_id from the client is never accepted (P20).
 */
import { NextResponse, type NextRequest } from "next/server";
import { getIdentityTokenCapabilities, listUserTokens, mintUserToken } from "../auth/adminApi";
import { currentUser } from "./currentUser";
import { readBoundedText } from "../boundedBody";
import { MAX_AUTH_REQUEST_BYTES } from "../proxyLimits";

export const dynamic = "force-dynamic";

const NO_STORE = { "Cache-Control": "no-store, no-cache, must-revalidate" } as const;
export async function GET() {
  const me = await currentUser();
  if (!me.ok) return NextResponse.json({ error: me.error }, { status: me.status, headers: NO_STORE });

  const listed = await listUserTokens(me.userId);
  if (!listed.ok) {
    return NextResponse.json({ error: listed.error || "Failed to list tokens" }, { status: listed.status || 502, headers: NO_STORE });
  }
  const capabilities = await getIdentityTokenCapabilities();
  if (!capabilities.ok || !capabilities.data) {
    return NextResponse.json(
      { error: capabilities.error || "Failed to negotiate token capabilities" },
      { status: capabilities.status || 503, headers: NO_STORE },
    );
  }
  return NextResponse.json({
    tokens: listed.data ?? [],
    available_scopes: capabilities.data.scopes,
    identity_contract: capabilities.data.version,
  }, { headers: NO_STORE });
}

export async function POST(request: NextRequest) {
  const me = await currentUser();
  if (!me.ok) return NextResponse.json({ error: me.error }, { status: me.status, headers: NO_STORE });

  let parsed: unknown;
  try {
    const raw = await readBoundedText(request, MAX_AUTH_REQUEST_BYTES);
    if (raw === null) {
      return NextResponse.json({ error: "Request body is too large" }, { status: 413, headers: NO_STORE });
    }
    parsed = JSON.parse(raw);
  } catch {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400, headers: NO_STORE });
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400, headers: NO_STORE });
  }
  const body = parsed as { scopes?: unknown; name?: unknown; expiresIn?: unknown };
  if (Object.keys(body).some((key) => !["scopes", "name", "expiresIn"].includes(key))) {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400, headers: NO_STORE });
  }

  const capabilities = await getIdentityTokenCapabilities();
  if (!capabilities.ok || !capabilities.data) {
    return NextResponse.json(
      { error: capabilities.error || "Failed to negotiate token capabilities" },
      { status: capabilities.status || 503, headers: NO_STORE },
    );
  }
  const validScopes = new Set(capabilities.data.scopes);
  const scopes = body.scopes;
  if (!Array.isArray(scopes)
      || scopes.length === 0
      || scopes.length > validScopes.size
      || scopes.some((scope) => typeof scope !== "string" || !validScopes.has(scope))
      || new Set(scopes).size !== scopes.length) {
    return NextResponse.json({ error: `Scopes must be a non-empty subset of ${[...validScopes].join(", ")}` }, { status: 400, headers: NO_STORE });
  }
  const validatedScopes = scopes as string[];
  if (body.name !== undefined && (typeof body.name !== "string" || body.name.trim().length > 255)) {
    return NextResponse.json({ error: "Token name must be at most 255 characters" }, { status: 400, headers: NO_STORE });
  }
  const name = typeof body.name === "string" && body.name.trim() ? body.name.trim() : undefined;
  if (body.expiresIn !== undefined && (
    typeof body.expiresIn !== "number"
    || !Number.isSafeInteger(body.expiresIn)
    || body.expiresIn <= 0
    || body.expiresIn > 10 * 365 * 24 * 60 * 60
  )) {
    return NextResponse.json({ error: "Token expiry must be a positive number of seconds up to 10 years" }, { status: 400, headers: NO_STORE });
  }
  const expiresIn = body.expiresIn as number | undefined;

  const minted = await mintUserToken(me.userId, {
    scopes: validatedScopes,
    name,
    expiresIn,
    contractVersion: capabilities.data.version,
  });
  if (!minted.ok || !minted.data?.token) {
    return NextResponse.json({ error: minted.error || "Failed to mint token" }, { status: minted.status || 502, headers: NO_STORE });
  }
  // The one and only time the secret crosses to the client.
  return NextResponse.json({ token: minted.data }, { status: 201, headers: NO_STORE });
}
