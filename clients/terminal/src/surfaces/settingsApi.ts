/** Settings → Models client edges. Per-user prefs ride the authenticated catch-all proxy to the
 *  gateway (`/api/user/models`, `/api/user/transcription` — admin-api behind it, secrets masked
 *  on every read-back). The GLOBAL defaults ride the admin-gated terminal route
 *  (`/api/admin/settings/{key}` — 404 for non-admins, indistinguishable from absent). */

export type ModelPrefs = {
  mode?: "subscription" | "custom" | null;
  model?: string | null;
  meeting_model?: string | null;
  base_url?: string | null;
  api_key_set?: boolean;
  api_key?: string | null; // masked on read (********abcd) — write-only in the clear
  config_status?: "valid" | "blocked" | "incomplete";
  validation_error?: string | null;
};

export type TranscriptionPrefs = {
  url?: string | null;
  token_set?: boolean;
  token?: string | null; // masked on read — write-only in the clear
  config_status?: "valid" | "blocked" | "incomplete";
  validation_error?: string | null;
};

/** Global platform settings carry the same fields, but write-only secrets are masked even for an
 * admin browser. An unchanged mask must be omitted; empty string explicitly clears. */
export type GlobalSetting = Record<string, string>;

async function jsonOrThrow(res: Response) {
  if (!res.ok) {
    let detail = `${res.status}`;
    try { detail = ((await res.json()) as { detail?: string; error?: string }).detail || detail; } catch { /* body not json */ }
    throw new Error(detail);
  }
  return res.json();
}

export async function getModelPrefs(): Promise<ModelPrefs> {
  return jsonOrThrow(await fetch("/api/user/models", { cache: "no-store" }));
}

export async function setModelPrefs(update: Partial<Record<keyof ModelPrefs, string>>): Promise<ModelPrefs> {
  return jsonOrThrow(await fetch("/api/user/models", {
    method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(update),
  }));
}

export async function getTranscriptionPrefs(): Promise<TranscriptionPrefs> {
  return jsonOrThrow(await fetch("/api/user/transcription", { cache: "no-store" }));
}

export async function setTranscriptionPrefs(update: { url?: string; token?: string }): Promise<TranscriptionPrefs> {
  return jsonOrThrow(await fetch("/api/user/transcription", {
    method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(update),
  }));
}

/** The admin-writable platform-settings keys: the two config domains + the first-run wizard's
 *  durable "setup" state. */
export type GlobalSettingKey = "models" | "transcription" | "setup";

/** null ⇒ caller is not an admin (the route 404s) — the global card simply doesn't render. */
export async function getGlobalSetting(key: GlobalSettingKey): Promise<GlobalSetting | null> {
  const res = await fetch(`/api/admin/settings/${key}`, { cache: "no-store" });
  if (res.status === 404) return null;
  const body = await jsonOrThrow(res) as { value?: GlobalSetting };
  return body.value ?? {};
}

export async function setGlobalSetting(key: GlobalSettingKey, update: GlobalSetting): Promise<GlobalSetting> {
  const res = await fetch(`/api/admin/settings/${key}`, {
    method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(update),
  });
  const body = await jsonOrThrow(res) as { value?: GlobalSetting };
  return body.value ?? {};
}

/** On-demand tests (agent-api /api/{models,transcription}/test via the gateway). A personal custom
 * tier may be probed live. An inherited platform/env tier returns generic managed/configured status
 * only: no operator credential spend and no origin, account, balance, or expiry disclosure. */
export type ConfigTestResult = {
  ok: boolean;
  summary: string;
  mode?: string;
  source?: "user" | "operator";
  managed?: boolean;
  expires_in_hours?: number; // personal result only
  account?: string;          // personal STT result only
  balance?: number | null;   // personal STT result only
};

export async function testModels(): Promise<ConfigTestResult> {
  return jsonOrThrow(await fetch("/api/models/test", { cache: "no-store" }));
}

export async function testTranscription(): Promise<ConfigTestResult> {
  return jsonOrThrow(await fetch("/api/transcription/test", { cache: "no-store" }));
}
