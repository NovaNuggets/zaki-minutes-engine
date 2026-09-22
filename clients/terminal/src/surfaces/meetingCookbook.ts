/** Compatibility entry point retained for callers while the reference Terminal is outside the
 * managed topology. It is deliberately inert: Hub owns the credential and the real composition. */
import { ManagedMinutesUnavailableError } from "./minutesApi";

export interface AgentOnMeetingInput {
  platform: string; // e.g. "google_meet"
  native_id: string; // the meeting's native id
  meeting_url?: string; // optional — derived for google_meet when absent
}

export interface AgentOnMeetingState {
  platform: string;
  native_id: string;
  bot: { sent: boolean; status?: string }; // meetings domain
  copilot: { enabled: boolean; resumed_from?: string; error?: string }; // agent domain
}

export async function agentOnMeeting(_input: AgentOnMeetingInput): Promise<AgentOnMeetingState> {
  throw new ManagedMinutesUnavailableError();
}
