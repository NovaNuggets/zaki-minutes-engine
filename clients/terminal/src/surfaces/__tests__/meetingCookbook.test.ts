/** The old cookbook entry point stays fail-closed in the reference Terminal. */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { agentOnMeeting } from "../meetingCookbook";

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn();
  globalThis.fetch = fetchMock as unknown as typeof fetch;
});
afterEach(() => vi.restoreAllMocks());

describe("agentOnMeeting — cookbook composition over two domain contracts", () => {
  it("is mechanically disabled in the reference Terminal and performs no network I/O", async () => {
    await expect(agentOnMeeting({ platform: "google_meet", native_id: "abc-defg-hij" }))
      .rejects.toThrow(/available in the ZAKI Hub/i);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
