/** Shared browser-proxy authentication policy used by Next routes and the custom WS server. */

/** @typedef {Record<string, string | undefined>} Environment */

function isLoopbackOrigin(raw) {
  if (!raw) return false;
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return false;
    const host = parsed.hostname.toLowerCase().replace(/^\[|\]$/g, "");
    return isLoopbackHost(host);
  } catch {
    return false;
  }
}

function isLoopbackHost(raw) {
  const host = String(raw || "").trim().toLowerCase().replace(/^\[|\]$/g, "");
  if (host === "localhost" || host.endsWith(".localhost") || host === "::1") return true;
  try {
    const octets = host.split(".");
    return octets.length === 4
      && octets.every((part) => /^\d+$/.test(part) && Number(part) <= 255)
      && Number(octets[0]) === 127;
  } catch {
    return false;
  }
}

/** Privileged passwordless modes need both a declared browser origin and the real host-side bind.
 * `HOST=0.0.0.0` is normal inside a container, so Compose/Lite may attest the Docker publication
 * bind with VEXA_TERMINAL_HOST_BIND; that value must itself be loopback. */
/** @param {Environment} env */
function privilegedLoopbackError(env) {
  const origins = [env.NEXTAUTH_URL, env.TERMINAL_URL]
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  if (origins.length === 0) {
    return "local privileged auth requires an explicit loopback Terminal origin";
  }
  if (origins.some((origin) => !isLoopbackOrigin(origin))) {
    return "local privileged auth is allowed only at a loopback Terminal origin";
  }
  const effectiveBind = String(env.VEXA_TERMINAL_HOST_BIND || env.HOST || "").trim();
  if (!isLoopbackHost(effectiveBind)) {
    return "local privileged auth requires a loopback Terminal listener/publication bind";
  }
  return null;
}

const DIRECT_EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** @param {Environment} [env] */
function configuredDirectLoginEmails(env = process.env) {
  return (env.VEXA_DIRECT_LOGIN_ALLOWED_EMAILS || "")
    .split(",")
    .map((email) => email.trim().toLowerCase())
    .filter(Boolean);
}

/** Fail-loud validation for the local-only exact direct-login allow policy. */
/** @param {Environment} [env] */
export function directLoginModeError(env = process.env) {
  const emails = configuredDirectLoginEmails(env);
  if (emails.length === 0) return null;
  if (emails.length > 100 || emails.some((email) => email.length > 320 || !DIRECT_EMAIL_RE.test(email))) {
    return "VEXA_DIRECT_LOGIN_ALLOWED_EMAILS must contain at most 100 valid exact email addresses";
  }
  if (new Set(emails).size !== emails.length) {
    return "VEXA_DIRECT_LOGIN_ALLOWED_EMAILS must not contain duplicate email addresses";
  }
  const localError = privilegedLoopbackError(env);
  return localError ? `VEXA_DIRECT_LOGIN_ALLOWED_EMAILS: ${localError}` : null;
}

/** Direct login is default-off and activates only for a valid, non-empty local allowlist. */
/** @param {Environment} [env] */
export function directLoginEnabled(env = process.env) {
  return configuredDirectLoginEmails(env).length > 0 && directLoginModeError(env) === null;
}

/** Exact, case-insensitive membership. Substring/pattern matching is intentionally forbidden. */
/** @param {unknown} email @param {Environment} [env] */
export function directLoginEmailAllowed(email, env = process.env) {
  if (!directLoginEnabled(env) || typeof email !== "string") return false;
  return configuredDirectLoginEmails(env).includes(email.trim().toLowerCase());
}

/** Return the fail-loud reason an explicitly requested shared-key mode cannot be activated. */
/** @param {Environment} [env] */
export function sharedKeyModeError(env = process.env) {
  if (env.VEXA_TERMINAL_SHARED_KEY_MODE !== "true") return null;
  if (!(env.VEXA_API_KEY || "").trim()) {
    return "VEXA_TERMINAL_SHARED_KEY_MODE requires a non-empty VEXA_API_KEY";
  }
  const localError = privilegedLoopbackError(env);
  return localError ? `VEXA_TERMINAL_SHARED_KEY_MODE: ${localError}` : null;
}

/** Deployment credentials are browser identity only in an explicit local single-user mode. */
/** @param {Environment} [env] */
export function sharedKeyModeEnabled(env = process.env) {
  return env.VEXA_TERMINAL_SHARED_KEY_MODE === "true" && sharedKeyModeError(env) === null;
}

/** Resolve one browser request's upstream key without implicitly spending a service credential. */
/** @param {string | undefined} cookieToken @param {Environment} [env] */
export function resolveTerminalProxyKey(cookieToken, env = process.env) {
  if (cookieToken) return cookieToken;
  return sharedKeyModeEnabled(env) ? (env.VEXA_API_KEY || "") : "";
}

/** Read one RFC6265 cookie without letting malformed percent encoding abort a WS upgrade. */
export function readCookieValue(header, name) {
  if (!header) return undefined;
  for (const part of header.split(";")) {
    const eq = part.indexOf("=");
    if (eq < 0 || part.slice(0, eq).trim() !== name) continue;
    try {
      return decodeURIComponent(part.slice(eq + 1).trim());
    } catch {
      return undefined;
    }
  }
  return undefined;
}
