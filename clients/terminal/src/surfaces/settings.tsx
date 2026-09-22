"use client";
/** Settings — the footer-gear CENTER tab (design-spec meeting-lifecycle-v2, W5): account-level
 *  configuration in one place — Calendar integration, API tokens, GitHub token, Account. The old
 *  "API Tokens" activity-bar item retired into here (its panels are imported, not duplicated);
 *  the Meetings sidebar keeps its own calendar connect UI at the point of need — this is the
 *  durable home. Sections are a left nav (no sub-routing; one tab, local state). */
import { useEffect, useState, type CSSProperties, type ReactNode } from "react";
import { registerTab } from "../contributions";
import { Icon } from "../ui-kit";
import { GitHubTokenCard, TokensPanel } from "./tokens";
import { getCalendarConfig, setCalendarConfig, getCalendarSyncStatus, syncCalendarNow, type CalendarConfig, type CalendarSyncStamp } from "./plannedApi";
import { getModelPrefs, setModelPrefs, getTranscriptionPrefs, setTranscriptionPrefs, getGlobalSetting, setGlobalSetting, testModels, testTranscription, type ConfigTestResult } from "./settingsApi";
import { getMinutesConfig, setMinutesConfig, type MinutesConfig, type MinutesConfigUpdate, type MinutesRetentionDays } from "./minutesApi";

type SectionId = "calendar" | "minutes" | "models" | "tokens" | "github" | "account";
const SECTIONS: Array<{ id: SectionId; label: string; icon: string }> = [
  { id: "calendar", label: "Calendar", icon: "cal" },
  { id: "minutes", label: "Minutes", icon: "mic" },
  { id: "models", label: "Models", icon: "spark" },
  { id: "tokens", label: "API tokens", icon: "key" },
  { id: "github", label: "GitHub", icon: "github" },
  { id: "account", label: "Account", icon: "user" },
];

const field: CSSProperties = { width: "100%", boxSizing: "border-box", fontSize: 12, padding: "6px 9px", borderRadius: 6, border: "1px solid var(--line)", background: "var(--panel2)", color: "var(--t1)" };
const btn: CSSProperties = { fontSize: 12, padding: "5px 12px", borderRadius: 6, border: "1px solid var(--line)", background: "var(--panel2)", color: "var(--t1)", cursor: "pointer" };

/** Minutes keeps the three authorities visible and distinct: the operator makes the product
 * available, the user consents to capture, and the user separately opts the agent into reads.
 * Retention is user-owned per artifact scope; no credential or internal-token field exists here. */
export function MinutesSection() {
  const [config, setConfig] = useState<MinutesConfig | null>(null);
  const [draft, setDraft] = useState<MinutesConfig | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    let alive = true;
    getMinutesConfig()
      .then((next) => { if (alive) { setConfig(next); setDraft(next); } })
      .catch((error: unknown) => alive && setErr(error instanceof Error ? error.message : String(error)));
    return () => { alive = false; };
  }, []);

  if (err && !draft) return <div role="alert" style={{ fontSize: 12, color: "var(--danger)" }}>⚠ Minutes settings unavailable: {err}</div>;
  if (!draft || !config) return <div role="status" style={{ fontSize: 12, color: "var(--t3)" }}>Loading Minutes settings…</div>;

  const captureOperatorAvailable = draft.operator_enabled === true;
  const readOperatorAvailable = draft.read_operator_enabled ?? captureOperatorAvailable;
  const captureRequested = draft.capture_requested ?? draft.capture_enabled;
  const agentReadRequested = draft.agent_read_requested ?? draft.agent_read_enabled;
  const storedCaptureRequested = config.capture_requested ?? config.capture_enabled;
  const storedAgentReadRequested = config.agent_read_requested ?? config.agent_read_enabled;
  const dirty = captureRequested !== storedCaptureRequested
    || agentReadRequested !== storedAgentReadRequested
    || (Object.keys(draft.retention_days) as Array<keyof MinutesRetentionDays>)
      .some((scope) => draft.retention_days[scope] !== config.retention_days[scope]);
  const retentionValid = draft.retention_days.audio <= draft.retention_days.transcript
    && draft.retention_days.summary <= draft.retention_days.transcript;
  const setRetention = (scope: keyof MinutesRetentionDays, raw: string) => {
    const days = Number(raw);
    if (!Number.isInteger(days) || days < 1 || days > 3650) return;
    setSaved(false);
    setDraft((current) => current && ({
      ...current,
      retention_days: { ...current.retention_days, [scope]: days },
    }));
  };
  const save = async () => {
    setBusy(true); setErr(null); setSaved(false);
    try {
      const update: MinutesConfigUpdate = {};
      if (captureRequested !== storedCaptureRequested) {
        update.capture_enabled = captureRequested;
      }
      if (agentReadRequested !== storedAgentReadRequested) {
        update.agent_read_enabled = agentReadRequested;
      }
      if ((Object.keys(draft.retention_days) as Array<keyof MinutesRetentionDays>)
        .some((scope) => draft.retention_days[scope] !== config.retention_days[scope])) {
        update.retention_days = draft.retention_days;
      }
      const next = await setMinutesConfig(update);
      const merged: MinutesConfig = {
        ...draft,
        ...next,
        operator_enabled: next.operator_enabled ?? config.operator_enabled,
        read_operator_enabled: next.read_operator_enabled ?? config.read_operator_enabled,
        policy_version: next.policy_version ?? config.policy_version,
        attested_at: "attested_at" in next ? next.attested_at : config.attested_at,
      };
      setConfig(merged); setDraft(merged); setSaved(true);
    } catch (error: unknown) {
      setErr(error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };
  const repairCapture = async () => {
    if (!draft.capture_repair || !captureRequested
      || (draft.capture_repair === "retention_repair_required" && !retentionValid)) return;
    setBusy(true); setErr(null); setSaved(false);
    try {
      const update: MinutesConfigUpdate = draft.capture_repair === "retention_repair_required"
        ? { capture_enabled: true, retention_days: draft.retention_days }
        : { capture_enabled: true };
      const next = await setMinutesConfig(update);
      const merged: MinutesConfig = {
        ...draft,
        ...next,
        operator_enabled: next.operator_enabled ?? config.operator_enabled,
        read_operator_enabled: next.read_operator_enabled ?? config.read_operator_enabled,
        policy_version: next.policy_version ?? config.policy_version,
        attested_at: "attested_at" in next ? next.attested_at : config.attested_at,
      };
      setConfig(merged); setDraft(merged); setSaved(true);
    } catch (error: unknown) {
      setErr(error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };
  const card: CSSProperties = { maxWidth: 520, border: "1px solid var(--line)", borderRadius: 9, padding: "11px 13px", background: "var(--panel)", marginBottom: 10 };
  const checkboxRow: CSSProperties = { display: "flex", alignItems: "flex-start", gap: 8, fontSize: 12.5, color: "var(--t2)", lineHeight: 1.45 };
  const availability = (available: boolean, label: string) => (
    <div style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 12.5, color: available ? "var(--green)" : "var(--t3)" }}>
      <span aria-hidden="true">{available ? "●" : "○"}</span>{label}
    </div>
  );

  return (
    <div style={{ maxWidth: 540 }}>
      <div style={{ fontSize: 11, color: "var(--t3)", lineHeight: 1.55, marginBottom: 12 }}>
        Minutes captures meetings as sensitive personal data. Availability is controlled by your
        operator; capture, agent access, and how long each artifact remains are your choices. This
        browser-scoped panel manages those choices; managed capture and erasure controls live in the ZAKI Hub.
      </div>

      <div style={card}>
        <div style={{ fontSize: 10.5, color: "var(--t3)", textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 5 }}>Operator availability</div>
        <div style={{ display: "grid", gap: 4 }}>
          {availability(captureOperatorAvailable, captureOperatorAvailable ? "Capture available from your operator" : "Capture not available from your operator")}
          {availability(readOperatorAvailable, readOperatorAvailable ? "Agent reads available" : "Agent reads unavailable")}
        </div>
        {(!captureOperatorAvailable || !readOperatorAvailable) && <div style={{ fontSize: 11, color: "var(--t3)", lineHeight: 1.45, marginTop: 5 }}>Unavailable capabilities cannot be enabled. Existing permissions can still be withdrawn, and retention remains your choice.</div>}
      </div>

      <div style={{ ...card, opacity: captureOperatorAvailable ? 1 : 0.65 }}>
        <div style={{ fontSize: 10.5, color: "var(--t3)", textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 8 }}>Your consent</div>
        <label style={{ ...checkboxRow, cursor: captureOperatorAvailable || storedCaptureRequested ? "pointer" : "default" }}>
          <input type="checkbox" checked={captureRequested} disabled={busy || (!captureOperatorAvailable && !storedCaptureRequested)}
            onChange={(event) => { setSaved(false); setDraft({ ...draft, capture_requested: event.target.checked }); }} />
          <span><b style={{ color: "var(--t1)" }}>Allow meeting capture</b><br />A visibly named ZAKI Notetaker may join meetings you explicitly send from the ZAKI Hub. You remain responsible for participant notice required in your jurisdiction.</span>
        </label>
      </div>

      <div style={{ ...card, opacity: readOperatorAvailable ? 1 : 0.65 }}>
        <div style={{ fontSize: 10.5, color: "var(--t3)", textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 8 }}>Agent access</div>
        <label style={{ ...checkboxRow, cursor: readOperatorAvailable || storedAgentReadRequested ? "pointer" : "default" }}>
          <input type="checkbox" checked={agentReadRequested} disabled={busy || (!readOperatorAvailable && !storedAgentReadRequested)}
            onChange={(event) => { setSaved(false); setDraft({ ...draft, agent_read_requested: event.target.checked }); }} />
          <span><b style={{ color: "var(--t1)" }}>Allow my agent to read meeting transcripts and summaries</b><br />This is separate from capture consent. The agent extracts governed knowledge through its own write pipeline; Minutes never writes directly to your brain.</span>
        </label>
      </div>

      <fieldset disabled={busy} style={{ ...card, display: "grid", gap: 8, borderColor: "var(--line)" }}>
        <legend style={{ fontSize: 10.5, color: "var(--t3)", textTransform: "uppercase", letterSpacing: ".06em", padding: "0 4px" }}>Retention by artifact</legend>
        {(["audio", "transcript", "summary"] as const).map((scope) => (
          <label key={scope} style={{ display: "grid", gridTemplateColumns: "155px 90px 1fr", alignItems: "center", gap: 8, fontSize: 12, color: "var(--t2)" }}>
            <span style={{ textTransform: "capitalize" }}>{scope} retention (days)</span>
            <input type="number" min={1} max={3650} step={1} value={draft.retention_days[scope]}
              onChange={(event) => setRetention(scope, event.target.value)} style={{ ...field, width: 90 }} />
            <span style={{ fontSize: 10.5, color: "var(--t3)" }}>{scope === "audio" ? "source media" : scope === "transcript" ? "verbatim text" : "derived minutes"}</span>
          </label>
        ))}
        <div style={{ fontSize: 10.5, color: "var(--t3)", lineHeight: 1.45 }}>Choose 1–3650 days per artifact. Audio and derived summaries cannot outlive the transcript; saving a separate workspace artifact is a different, governed choice. In the ZAKI Hub, permanent delete erases audio, transcript, and summary together; stopping a live capture also withdraws consent and fences further ingest.</div>
        {!retentionValid && <div role="alert" style={{ fontSize: 10.5, color: "var(--danger)", lineHeight: 1.45 }}>Audio and summary retention must each be less than or equal to transcript retention.</div>}
      </fieldset>

      {captureRequested && draft.capture_repair && (
        <div role="status" style={{ ...card, borderColor: "var(--warn)", color: "var(--t2)", fontSize: 11.5, lineHeight: 1.5 }}>
          <div style={{ marginBottom: 8 }}>
            {draft.capture_repair === "retention_repair_required"
              ? "Your stored retention policy could not be validated, so capture is safely paused. Review the displayed defaults, then repair retention and consent to the current policy."
              : "Minutes capture is safely paused because its consent attestation is missing or belongs to an older policy. Consent again to the current policy to resume."}
          </div>
          <button disabled={busy || (draft.capture_repair === "retention_repair_required" && !retentionValid)} onClick={() => void repairCapture()}
            style={{ ...btn, background: "var(--accent)", color: "var(--on-accent)", border: "none" }}>
            {busy ? "Repairing…" : draft.capture_repair === "retention_repair_required"
              ? "Repair retention and re-consent"
              : "Re-consent to Minutes capture"}
          </button>
        </div>
      )}

      {err && <div role="alert" style={{ fontSize: 11.5, color: "var(--danger)", marginBottom: 8 }}>⚠ {err}</div>}
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <button disabled={busy || !dirty || !retentionValid} onClick={() => void save()}
          style={{ ...btn, background: dirty && retentionValid ? "var(--accent)" : "var(--panel2)", color: dirty && retentionValid ? "var(--on-accent)" : "var(--t3)", border: dirty && retentionValid ? "none" : btn.border, opacity: busy ? 0.55 : 1 }}>
          {busy ? "Saving…" : "Save Minutes settings"}
        </button>
        {saved && <span role="status" style={{ fontSize: 11.5, color: "var(--green)" }}>Saved</span>}
      </div>
    </div>
  );
}

export interface ConfigField {
  key: string;
  label: string;
  placeholder?: string;
  secret?: boolean;
  options?: Array<{ value: string; label: string }>;
  showIf?: (values: Record<string, string>) => boolean;
  /** Fields whose stored values must be explicitly cleared when this option is selected. */
  clearKeysWhen?: (value: string) => string[];
}

export const MODEL_FIELDS: ConfigField[] = [
  { key: "mode", label: "Provider", options: [
    { value: "", label: "Deployment default" },
    { value: "subscription", label: "Claude subscription (deployment credentials)" },
    { value: "custom", label: "Custom endpoint (open-source / gateway)" },
  ], clearKeysWhen: (value) => value === "custom" ? [] : ["base_url", "api_key"] },
  { key: "base_url", label: "Base URL", placeholder: "https://… (Anthropic/OpenAI-compatible gateway)", showIf: (v) => v.mode === "custom" },
  { key: "api_key", label: "API key", placeholder: "unchanged unless typed", secret: true, showIf: (v) => v.mode === "custom" },
  { key: "model", label: "Chat model", placeholder: "deployment default (e.g. sonnet)" },
  { key: "meeting_model", label: "Meeting model", placeholder: "defaults to chat model" },
];

/** Calendar integration — an import-only ICS feed. Capture remains a separate, explicit managed
 *  ZAKI Notetaker action; the launch backend does not auto-join imported meetings. */
function CalendarSection() {
  const [cfg, setCfg] = useState<CalendarConfig | null>(null);
  const [stamp, setStamp] = useState<CalendarSyncStamp | null>(null);
  const [url, setUrl] = useState("");
  const [busy, setBusy] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const refresh = () => {
    getCalendarConfig().then((c) => { setCfg(c); setErr(null); }).catch((e: unknown) => setErr(e instanceof Error ? e.message : String(e)));
    getCalendarSyncStatus().then(setStamp).catch(() => undefined);
  };
  useEffect(refresh, []);

  const save = async (body: { ics_url?: string | null }) => {
    setBusy(true); setErr(null);
    try { setCfg(await setCalendarConfig(body)); setUrl(""); refresh(); }
    catch (e: unknown) { setErr(e instanceof Error ? e.message : String(e)); }
    finally { setBusy(false); }
  };
  const syncNow = async () => {
    setSyncing(true); setErr(null);
    try { setStamp(await syncCalendarNow()); }
    catch (e: unknown) { setErr(e instanceof Error ? e.message : String(e)); }
    finally { setSyncing(false); }
  };

  const connected = !!cfg?.ics_url_set;
  return (
    <div>
      <div style={{ fontSize: 11, color: "var(--t3)", lineHeight: 1.5, marginBottom: 12, maxWidth: 460 }}>
        Connect your calendar's secret ICS feed and scheduled meetings appear in Meetings by themselves.
        Use the ZAKI Hub when you want to start managed capture.
      </div>
      {err && <div role="alert" style={{ fontSize: 11.5, color: "var(--danger)", marginBottom: 10 }}>⚠ {err}</div>}
      {connected ? (
        <div style={{ display: "flex", flexDirection: "column", gap: 10, maxWidth: 460 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <Icon name="cal" size={13} style={{ color: "var(--green)" }} />
            <span style={{ flex: 1, fontSize: 12.5, color: "var(--t2)", fontFamily: "var(--mono)" }}>{cfg?.ics_url_masked ?? "connected"}</span>
            <button disabled={busy || syncing} onClick={() => void syncNow()} style={btn}>{syncing ? "Syncing…" : "Sync now"}</button>
            <button disabled={busy || syncing} onClick={() => void save({ ics_url: null })} style={{ ...btn, color: "var(--danger)" }}>Disconnect</button>
          </div>
          {stamp?.last_error
            ? <div role="alert" style={{ fontSize: 11.5, color: "var(--danger)", lineHeight: 1.5 }}>⚠ Last sync failed: {stamp.last_error}</div>
            : stamp?.last_sync && <div style={{ fontSize: 11, color: "var(--t3)" }}>Last synced {new Date(stamp.last_sync).toLocaleString()}</div>}
        </div>
      ) : (
        <div style={{ display: "flex", gap: 8, maxWidth: 460 }}>
          <input value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://calendar.google.com/…/basic.ics (secret address)"
            onKeyDown={(e) => { if (e.key === "Enter" && url.trim()) void save({ ics_url: url.trim() }); }} style={field} />
          <button disabled={busy || !url.trim()} onClick={() => void save({ ics_url: url.trim() })}
            style={{ ...btn, background: "var(--accent)", color: "var(--on-accent)", border: "none", opacity: busy || !url.trim() ? 0.5 : 1, flex: "none" }}>
            {busy ? "Connecting…" : "Connect"}
          </button>
        </div>
      )}
    </div>
  );
}

/** One models/transcription config form — the SAME fields serve the per-user prefs and (for
 *  admins) the global platform defaults; only load/save differ. Secrets arrive MASKED
 *  (********abcd): an untouched masked value is never sent back, typing replaces it, emptying a
 *  previously-set field clears it (empty string = clear, the API's contract). */
export function ConfigForm({ fields, load, save, note }: {
  fields: ConfigField[];
  load: () => Promise<Record<string, string>>;
  save: (update: Record<string, string>) => Promise<Record<string, string>>;
  note?: string;
}) {
  const [values, setValues] = useState<Record<string, string>>({});
  const [initial, setInitial] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    let on = true;
    load().then((v) => { if (on) { setValues(v); setInitial(v); } })
      .catch((e: unknown) => on && setErr(e instanceof Error ? e.message : String(e)));
    return () => { on = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const dirty = fields.some((f) => (values[f.key] ?? "") !== (initial[f.key] ?? ""));
  const doSave = async () => {
    setErr(null); setSaved(false);
    // Send only what changed; an untouched masked secret stays server-side.
    const update: Record<string, string> = {};
    for (const f of fields) {
      const next = values[f.key] ?? "";
      if (next === (initial[f.key] ?? "")) continue;
      if (f.secret && next.startsWith("********")) {
        setErr(`Replace the masked ${f.label.toLowerCase()} completely, or leave it unchanged.`);
        return;
      }
      update[f.key] = next;
    }
    setBusy(true);
    try { const v = await save(update); setValues(v); setInitial(v); setSaved(true); }
    catch (e: unknown) { setErr(e instanceof Error ? e.message : String(e)); }
    finally { setBusy(false); }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8, maxWidth: 460 }}>
      {note && <div style={{ fontSize: 11, color: "var(--t3)", lineHeight: 1.5 }}>{note}</div>}
      {err && <div role="alert" style={{ fontSize: 11.5, color: "var(--danger)" }}>⚠ {err}</div>}
      {(values.config_status === "blocked" || values.config_status === "incomplete") && (
        <div role="alert" style={{ fontSize: 11.5, color: "var(--danger)", lineHeight: 1.5 }}>
          ⚠ {values.validation_error || "This personal provider configuration cannot be used."}
        </div>
      )}
      {fields.map((f) => (f.showIf && !f.showIf(values)) ? null : (
        <label key={f.key} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12, color: "var(--t2)" }}>
          <span style={{ width: 110, flex: "none", color: "var(--t3)" }}>{f.label}</span>
          {f.options ? (
            <select value={values[f.key] ?? ""}
              onChange={(e) => {
                setSaved(false);
                const next = e.target.value;
                setValues((valuesBefore) => {
                  const updated = { ...valuesBefore, [f.key]: next };
                  for (const key of f.clearKeysWhen?.(next) ?? []) updated[key] = "";
                  return updated;
                });
              }}
              style={{ ...field, width: "auto", flex: 1 }}>
              {f.options.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          ) : (
            <input value={values[f.key] ?? ""} placeholder={f.placeholder}
              type={f.secret ? "password" : "text"}
              onChange={(e) => { setSaved(false); setValues((v) => ({ ...v, [f.key]: e.target.value })); }}
              style={field} />
          )}
        </label>
      ))}
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <button disabled={busy || !dirty} onClick={() => void doSave()}
          style={{ ...btn, background: dirty ? "var(--accent)" : "var(--panel2)", color: dirty ? "var(--on-accent)" : "var(--t3)", border: dirty ? "none" : btn.border, opacity: busy ? 0.5 : 1 }}>
          {busy ? "Saving…" : "Save"}
        </button>
        {saved && <span style={{ fontSize: 11.5, color: "var(--green)" }}>Saved — next agent turn uses it</span>}
      </div>
    </div>
  );
}

/** On-demand credential test row. Personal endpoints get a live probe; inherited platform/env
 * credentials return only generic operator-managed status, with no credential spend or disclosure
 * of origin/account/balance. */
function TestRow({ label, run }: { label: string; run: () => Promise<ConfigTestResult> }) {
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState<ConfigTestResult | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const doTest = async () => {
    setBusy(true); setErr(null); setRes(null);
    try { setRes(await run()); }
    catch (e: unknown) { setErr(e instanceof Error ? e.message : String(e)); }
    finally { setBusy(false); }
  };
  const provenance = res ? [res.mode, res.source && `via ${res.source}`].filter(Boolean).join(" · ") : "";
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4, maxWidth: 460, marginTop: 6 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <button disabled={busy} onClick={() => void doTest()}
          style={{ ...btn, opacity: busy ? 0.5 : 1 }}>
          {busy ? "Testing…" : label}
        </button>
        {res && (
          <span style={{ fontSize: 11.5, color: res.ok ? "var(--green)" : "var(--danger)" }}>
            {res.ok ? "✓" : "✗"} {provenance && <span style={{ color: "var(--t3)" }}>[{provenance}] </span>}
            {res.summary}
          </span>
        )}
        {err && <span role="alert" style={{ fontSize: 11.5, color: "var(--danger)" }}>⚠ test failed: {err}</span>}
      </div>
    </div>
  );
}

/** Models — which LLM the agent runs on and which STT backend the bot transcribes with; your own
 *  settings first, the deployment-wide defaults below for admins. Empty fields = the level below
 *  decides (global settings, then the deployment env). */
function ModelsSection() {
  const [globalAdmin, setGlobalAdmin] = useState(false);
  useEffect(() => {
    let on = true;
    // Admin probe: the global card renders only when /api/admin/settings answers (404 = not admin).
    getGlobalSetting("models").then((v) => on && setGlobalAdmin(v !== null)).catch(() => undefined);
    return () => { on = false; };
  }, []);

  const modelFields = MODEL_FIELDS;
  const transcriptionFields = [
    { key: "url", label: "Service URL", placeholder: "deployment default" },
    { key: "token", label: "Token", placeholder: "unchanged unless typed", secret: true },
  ];
  const asStrings = (v: Record<string, unknown>): Record<string, string> => {
    const out: Record<string, string> = {};
    for (const [k, val] of Object.entries(v)) if (typeof val === "string" && val) out[k] = val;
    return out;
  };
  const head: CSSProperties = { fontSize: 12, fontWeight: 600, color: "var(--t1)", margin: "14px 0 6px" };

  return (
    <div>
      <div style={{ fontSize: 11, color: "var(--t3)", lineHeight: 1.5, marginBottom: 12, maxWidth: 460 }}>
        Which model the agent runs on, and which transcription service meeting bots use. Provider
        &ldquo;subscription&rdquo; rides the deployment&rsquo;s Claude credentials; &ldquo;custom&rdquo; points at your own
        Anthropic/OpenAI-compatible endpoint (a LiteLLM/OpenRouter gateway serves open-source
        models). A personal transcription URL must be operator-approved and uses only its own token;
        it never inherits the deployment&rsquo;s STT credential.
      </div>
      <div style={head}>Your models</div>
      <ConfigForm fields={modelFields} load={async () => asStrings(await getModelPrefs())}
        save={async (u) => asStrings(await setModelPrefs(u))} />
      <TestRow label="Test model credentials" run={testModels} />
      <div style={head}>Your transcription backend</div>
      <ConfigForm fields={transcriptionFields} load={async () => asStrings(await getTranscriptionPrefs())}
        save={async (u) => asStrings(await setTranscriptionPrefs(u))} />
      <TestRow label="Test transcription backend" run={testTranscription} />
      {globalAdmin && <>
        <div style={{ ...head, marginTop: 22, color: "var(--accent)" }}>Global defaults (admin — every user without own settings)</div>
        <ConfigForm fields={modelFields} load={async () => (await getGlobalSetting("models")) ?? {}}
          save={(u) => setGlobalSetting("models", u)} />
        <div style={head}>Global transcription backend</div>
        <ConfigForm fields={transcriptionFields} load={async () => (await getGlobalSetting("transcription")) ?? {}}
          save={(u) => setGlobalSetting("transcription", u)} />
      </>}
    </div>
  );
}

function AccountSection() {
  const [user, setUser] = useState<{ email?: string | null; name?: string | null } | null>(null);
  useEffect(() => {
    let on = true;
    fetch("/api/auth/me", { cache: "no-store" })
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => on && setUser((d?.user as { email?: string; name?: string } | undefined) ?? null))
      .catch(() => undefined);
    return () => { on = false; };
  }, []);
  return (
    <div style={{ fontSize: 12.5, color: "var(--t2)", lineHeight: 1.9 }}>
      <div><span style={{ color: "var(--t3)" }}>Signed in as</span> <span style={{ color: "var(--t1)" }}>{user?.name || user?.email || "…"}</span></div>
      {user?.email && <div><span style={{ color: "var(--t3)" }}>Email</span> <span style={{ fontFamily: "var(--mono)" }}>{user.email}</span></div>}
      <div style={{ color: "var(--t3)", marginTop: 6 }}>Theme and sign-out live next to your name in the footer.</div>
    </div>
  );
}

function SettingsView() {
  const [section, setSection] = useState<SectionId>("calendar");
  const bodies: Record<SectionId, ReactNode> = {
    calendar: <CalendarSection />,
    minutes: <MinutesSection />,
    models: <ModelsSection />,
    tokens: <TokensPanel />,
    github: <GitHubTokenCard />,
    account: <AccountSection />,
  };
  return (
    <div style={{ height: "100%", display: "flex", minHeight: 0 }}>
      <div style={{ width: 160, flex: "none", borderRight: "1px solid var(--line)", padding: "14px 8px", background: "var(--sidebar)" }}>
        <div style={{ fontSize: 15, fontWeight: 700, color: "var(--t1)", padding: "0 8px 10px" }}>Settings</div>
        {SECTIONS.map((s) => (
          <button key={s.id} onClick={() => setSection(s.id)}
            style={{ display: "flex", alignItems: "center", gap: 7, width: "100%", textAlign: "left", fontSize: 12.5,
              padding: "6px 9px", borderRadius: 7, border: "none", cursor: "pointer",
              color: section === s.id ? "var(--t1)" : "var(--t2)", background: section === s.id ? "var(--panel2)" : "transparent" }}>
            <Icon name={s.icon} size={13} />{s.label}
          </button>
        ))}
      </div>
      <div style={{ flex: 1, overflowY: "auto", padding: "18px 22px", minWidth: 0 }}>
        <div style={{ fontSize: 13.5, fontWeight: 600, color: "var(--t1)", marginBottom: 12 }}>
          {SECTIONS.find((s) => s.id === section)?.label}
        </div>
        {bodies[section]}
      </div>
    </div>
  );
}

registerTab("settings", SettingsView);
