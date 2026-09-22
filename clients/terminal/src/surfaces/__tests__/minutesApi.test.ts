import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  getMinutesConfig,
  getMinutesMeetingStatus,
  setMinutesConfig,
  startMinutesCapture,
  withdrawMinutesCapture,
  eraseMinutesMeeting,
} from "../minutesApi";

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn(async () => ({
    ok: true,
    status: 200,
    json: async () => ({
      operator_enabled: true,
      capture_enabled: false,
      agent_read_enabled: false,
      retention_days: { audio: 7, transcript: 30, summary: 30 },
    }),
  }) as Response);
  globalThis.fetch = fetchMock as unknown as typeof fetch;
});

afterEach(() => vi.restoreAllMocks());

describe("Minutes API client", () => {
  it("fails every managed mutation/status helper locally because this Terminal has no Hub credential", async () => {
    await expect(startMinutesCapture({ platform: "google_meet", native_meeting_id: "abc-defg-hij" }))
      .rejects.toThrow(/available in the ZAKI Hub/i);
    await expect(withdrawMinutesCapture("google_meet", "abc-defg-hij"))
      .rejects.toThrow(/available in the ZAKI Hub/i);
    await expect(eraseMinutesMeeting("42")).rejects.toThrow(/available in the ZAKI Hub/i);
    await expect(getMinutesMeetingStatus("42")).rejects.toThrow(/available in the ZAKI Hub/i);
    expect(fetchMock).not.toHaveBeenCalled();
  });
  it("reads the signed-in user's Minutes posture without caching it", async () => {
    await expect(getMinutesConfig()).resolves.toMatchObject({
      operator_enabled: true,
      capture_enabled: false,
      agent_read_enabled: false,
    });
    expect(fetchMock).toHaveBeenCalledWith("/api/user/minutes", { cache: "no-store" });
  });

  it("writes only user-owned consent, read opt-in, and retention choices", async () => {
    await setMinutesConfig({
      capture_enabled: true,
      agent_read_enabled: false,
      retention_days: { audio: 1, transcript: 14, summary: 14 },
    });

    expect(fetchMock).toHaveBeenCalledWith("/api/user/minutes", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        capture_enabled: true,
        agent_read_enabled: false,
        retention_days: { audio: 1, transcript: 14, summary: 14 },
      }),
    });
  });

  it("turns a sanitized proxy failure into user-facing copy", async () => {
    fetchMock.mockResolvedValueOnce({
      ok: false,
      status: 502,
      json: async () => ({ error: "upstream_unreachable" }),
    } as Response);

    await expect(getMinutesConfig()).rejects.toThrow(/temporarily unavailable/i);
  });
});
