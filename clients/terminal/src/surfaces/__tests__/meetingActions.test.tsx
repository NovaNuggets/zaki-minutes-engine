/** Behavioral test for the dropdown ACTION→TRANSITION map (meeting.tsx `actionsFor`).
 *
 *  For each REAL status the row offers a specific action set, and each action fires EXACTLY ONE endpoint
 *  with the right method + body. We assert both the offered set and the fetch each `run()` performs.
 *  The `scheduled`-intent body uses the same flat `intent` PUT the producer (meeting-api) accepts.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  actionsFor,
  canShowMeetingHeaderOwnerControls,
  canShowRowActions,
  shouldShowMeetingStatusBadge,
} from "../meeting";
import type { MeetingMock } from "../meetingModel";

const NATIVE = "abc-defg-hij";

function row(live_status: string): MeetingMock {
  return {
    id: NATIVE,
    native_id: NATIVE,
    title: "Google Meet · " + NATIVE,
    when: "now",
    status: "past",
    live_status,
    platform: "Google Meet",
    has_recording: false,
    docs: [],
    participants: [],
    mentioned: [],
    actions: [],
    transcript: [],
    insights: [],
  } as MeetingMock;
}

let fetchMock: ReturnType<typeof vi.fn>;
function lastFetch() {
  const c = fetchMock.mock.calls.at(-1)!;
  return { url: String(c[0]), init: (c[1] ?? {}) as RequestInit, body: c[1]?.body ? JSON.parse(String(c[1].body)) : undefined };
}

beforeEach(() => {
  fetchMock = vi.fn(async () => ({ ok: true, json: async () => ({}) }) as Response);
  globalThis.fetch = fetchMock as unknown as typeof fetch;
});
afterEach(() => vi.restoreAllMocks());

describe("actionsFor — offered action sets per status", () => {
  const ids = (s: string) => actionsFor(row(s)).map((a) => a.id);

  it("never exposes managed capture, withdrawal, or erasure controls in the reference Terminal", () => {
    for (const status of ["idle", "scheduled", "requested", "joining", "active", "completed", "failed", "stopped"]) {
      for (const unsupported of ["send", "resend", "stop", "erase"]) {
        expect(ids(status)).not.toContain(unsupported);
      }
    }
  });

  it("idle → Schedule + Delete", () => expect(ids("idle")).toEqual(["schedule", "delete"]));
  it("scheduled → Cancel + Delete", () => expect(ids("scheduled")).toEqual(["cancel", "delete"]));
  it("link-less planned rows → row-id actions only (no native path exists)", () => {
    const linkless = (s: string) => actionsFor({ ...row(s), native_id: undefined, id: "42" });
    expect(linkless("idle").map((a) => a.id)).toEqual(["delete"]);
    expect(linkless("scheduled").map((a) => a.id)).toEqual(["cancel", "delete"]);
  });
  it("active has no unsupported lifecycle control", () => expect(ids("active")).toEqual([]));
  it("joining/awaiting/needs_human_help/stopping have no unsupported lifecycle control", () => {
    for (const s of ["requested", "joining", "awaiting_admission", "needs_human_help", "stopping"]) {
      expect(ids(s)).toEqual([]);
      expect(shouldShowMeetingStatusBadge(row(s))).toBe(true);
    }
  });
  it("terminal and unknown rows expose no managed re-send or erasure controls", () => {
    for (const s of ["completed", "failed", "stopped", "future_status"]) {
      const terminal = { ...row(s), id: "42" };
      expect(actionsFor(terminal)).toEqual([]);
      expect(canShowRowActions(terminal)).toBe(false);
    }
  });
  it("shared meeting headers never expose owner lifecycle or transcript-share controls", () => {
    expect(canShowMeetingHeaderOwnerControls(row("active"))).toBe(true);
    expect(canShowMeetingHeaderOwnerControls({ ...row("active"), shared: true })).toBe(false);
  });
});

describe("actionsFor — each action fires the correct endpoint+body", () => {
  it("scheduled→Cancel PUTs intent:idle to the intent route", () => {
    actionsFor(row("scheduled")).find((a) => a.id === "cancel")!.run();
    const { url, init, body } = lastFetch();
    expect(url).toBe(`/api/meetings/google_meet/${NATIVE}/intent`);
    expect(init.method).toBe("PUT");
    expect(body).toEqual({ intent: "idle" });
  });

  it("planned-row actions report network failures instead of throwing", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const onFailure = vi.fn();
    fetchMock.mockRejectedValueOnce(new TypeError("Failed to fetch"));

    await expect(actionsFor(row("idle")).find((a) => a.id === "delete")!.run(onFailure)).resolves.toBeUndefined();

    expect(onFailure).toHaveBeenCalledWith({
      actionId: "delete",
      actionLabel: "Delete",
      native: NATIVE,
      message: "Failed to fetch",
    });
    expect(warn).toHaveBeenCalledWith("meeting action failed", expect.objectContaining({ actionId: "delete", message: "Failed to fetch" }));
  });

  it("idle→Schedule PUTs intent:scheduled with an ISO `at`", () => {
    const at = "2026-06-25T18:00:00.000Z";
    vi.spyOn(window, "prompt").mockReturnValue("2026-06-25 18:00");
    vi.spyOn(Date.prototype, "toISOString").mockReturnValue(at);
    actionsFor(row("idle")).find((a) => a.id === "schedule")!.run();
    const { url, init, body } = lastFetch();
    expect(url).toBe(`/api/meetings/google_meet/${NATIVE}/intent`);
    expect(init.method).toBe("PUT");
    expect(body.intent).toBe("scheduled");
    expect(body.at).toBe(at);
  });

  it("planned→Delete DELETEs by ROW id (works link-less)", () => {
    actionsFor({ ...row("idle"), native_id: undefined, id: "42" }).find((a) => a.id === "delete")!.run();
    const { url, init } = lastFetch();
    expect(url).toBe("/api/meetings/42");
    expect(init.method).toBe("DELETE");
  });

  it("link-less scheduled→Cancel PATCHes scheduled_at:null by ROW id", () => {
    actionsFor({ ...row("scheduled"), native_id: undefined, id: "42" }).find((a) => a.id === "cancel")!.run();
    const { url, init, body } = lastFetch();
    expect(url).toBe("/api/meetings/42");
    expect(init.method).toBe("PATCH");
    expect(body).toEqual({ scheduled_at: null });
  });

});
