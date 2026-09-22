/** Reference-Terminal Minutes client. This browser-scoped surface manages consent and retention.
 * Managed capture/status/withdrawal/erasure require a dedicated Hub credential that is deliberately
 * absent here, so those compatibility helpers fail locally without performing network I/O. */

export type MinutesRetentionDays = {
  audio: number;
  transcript: number;
  summary: number;
};

export type MinutesConfig = {
  capture_enabled: boolean;
  agent_read_enabled: boolean;
  capture_requested: boolean;
  agent_read_requested: boolean;
  retention_days: MinutesRetentionDays;
  operator_enabled?: boolean;
  read_operator_enabled?: boolean;
  policy_version?: string;
  attested_at?: string | null;
  capture_repair?: "reconsent_required" | "retention_repair_required" | null;
};

export type MinutesConfigUpdate = Partial<Pick<
  MinutesConfig,
  "capture_enabled" | "agent_read_enabled" | "retention_days"
>>;

export type MinutesCaptureInput = {
  platform: string;
  native_meeting_id: string;
  meeting_url?: string;
};

export type MinutesCaptureReceipt = {
  id?: string;
  meeting_id?: string;
  status?: string;
  state?: string;
};

export type MinutesMeetingStatus = {
  meeting_id: string;
  status: "requested" | "joining" | "awaiting_admission" | "active" |
    "needs_human_help" | "stopping" | "completed" | "failed";
  completion_reason?: string;
  failure_stage?: string;
};

const SETTINGS_ERROR_COPY: Record<string, string> = {
  request_too_large: "The Minutes request was too large.",
  upstream_error: "Minutes settings could not complete that request. Try again.",
  upstream_unreachable: "Minutes is temporarily unavailable. Try again shortly.",
  upstream_response_too_large: "Minutes returned too much data to display safely.",
};

export class MinutesApiError extends Error {
  constructor(public readonly status: number, public readonly code: string | undefined, message: string) {
    super(message);
    this.name = "MinutesApiError";
  }
}

export class ManagedMinutesUnavailableError extends Error {
  constructor() {
    super("Managed Minutes controls are available in the ZAKI Hub, not this reference Terminal.");
    this.name = "ManagedMinutesUnavailableError";
  }
}

export function minutesErrorText(error: unknown): string {
  return error instanceof Error && error.message ? error.message : "Minutes request failed.";
}

async function jsonOrThrow<T>(response: Response): Promise<T> {
  const body = await response.json().catch(() => undefined) as T | {
    detail?: unknown;
    error?: string | { code?: unknown };
  } | undefined;
  if (!response.ok) {
    const errorBody = body as { detail?: unknown; error?: string | { code?: unknown } } | undefined;
    const code = typeof errorBody?.error === "string"
      ? errorBody.error
      : typeof errorBody?.error === "object" && typeof errorBody.error.code === "string"
        ? errorBody.error.code
        : undefined;
    const detail = typeof errorBody?.detail === "string" ? errorBody.detail : undefined;
    throw new MinutesApiError(
      response.status,
      code,
      (code && SETTINGS_ERROR_COPY[code]) || detail || code || `Minutes request failed (${response.status}).`,
    );
  }
  return body as T;
}

export async function getMinutesConfig(): Promise<MinutesConfig> {
  return jsonOrThrow(await fetch("/api/user/minutes", { cache: "no-store" }));
}

export async function setMinutesConfig(update: MinutesConfigUpdate): Promise<MinutesConfig> {
  return jsonOrThrow(await fetch("/api/user/minutes", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(update),
  }));
}

export async function startMinutesCapture(_input: MinutesCaptureInput): Promise<MinutesCaptureReceipt> {
  throw new ManagedMinutesUnavailableError();
}

export async function withdrawMinutesCapture(
  _platform: string,
  _nativeMeetingId: string,
): Promise<MinutesCaptureReceipt> {
  throw new ManagedMinutesUnavailableError();
}

export async function eraseMinutesMeeting(_rowId: number | string): Promise<MinutesCaptureReceipt> {
  throw new ManagedMinutesUnavailableError();
}

export async function getMinutesMeetingStatus(
  _rowId: number | string,
): Promise<MinutesMeetingStatus> {
  throw new ManagedMinutesUnavailableError();
}
