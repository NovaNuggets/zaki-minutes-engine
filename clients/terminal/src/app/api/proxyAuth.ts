/** Per-user API key for the upstream proxies.
 *
 *  The X-API-Key the terminal forwards to the gateway / agent-api is the logged-in user's APIToken,
 *  read from the httpOnly `vexa-token` cookie (set by /api/auth/login). Without it, hosted mode
 *  rejects locally. An explicit loopback-only shared-key mode may use VEXA_API_KEY; deployment
 *  service keys such as VEXA_BOT_API_KEY are never browser identity. */
import { cookies } from "next/headers";
import { AUTH_COOKIE, validateAuthToken } from "./auth/adminApi";
import { resolveTerminalProxyKey } from "../../proxyAuthPolicy.mjs";

async function cookieApiKey(): Promise<string> {
  try {
    return (await cookies()).get(AUTH_COOKIE)?.value ?? "";
  } catch {
    return "";
  }
}

/** Resolve the browser's API key. Deployment service keys are not browser identity. */
export async function resolveApiKey(): Promise<string> {
  return resolveTerminalProxyKey(await cookieApiKey(), process.env);
}

/** Resolve and validate a token before a terminal-owned route spends a deployment credential.
 * Gateway proxy routes can let the gateway validate X-API-Key itself; direct operator-backed
 * egress (such as STT) has no such downstream auth gate and must fail closed here. */
export async function resolveValidatedApiKey(): Promise<string> {
  return (await resolveValidatedPrincipal())?.apiKey ?? "";
}

export interface ValidatedPrincipal {
  apiKey: string;
  userId: string | number;
  email: string;
  isAdmin: boolean;
}

/** Validated token plus server-derived identity for routes that also need scoped internal config. */
export async function resolveValidatedPrincipal(): Promise<ValidatedPrincipal | null> {
  // Hosted mode resolves only the httpOnly login cookie. The sole alternative is the explicit
  // loopback shared-user key accepted by resolveTerminalProxyKey; it is still validated by the
  // identity oracle below before any operator credential is spent. Generic deployment/bot keys
  // never enter this path.
  const apiKey = await resolveApiKey();
  if (!apiKey) return null;
  const validated = await validateAuthToken(apiKey);
  if (!validated.ok) return null;
  return {
    apiKey,
    userId: validated.userId,
    email: validated.email,
    isAdmin: validated.isAdmin,
  };
}
