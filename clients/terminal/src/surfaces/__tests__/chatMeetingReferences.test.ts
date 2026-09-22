import { describe, expect, it } from "vitest";

import { referenceContext } from "../chat";

describe("meeting reference grounding", () => {
  it("uses the canonical numeric row for both workspace and transcript reads", () => {
    const context = referenceContext("Please inspect @meeting:42");
    expect(context).toContain("notes_workspace_path: kg/entities/meeting/42.md");
    expect(context).toContain("transcript_api_path: /api/transcripts/by-id/42");
    expect(context).not.toContain("native_id:");
  });

  it("never turns a native or hostile token into a workspace path", () => {
    const context = referenceContext("Please inspect @meeting:abc-defg-hij");
    expect(context).toContain("not a canonical numeric meeting-row reference");
    expect(context).not.toContain("kg/entities/meeting/abc-defg-hij.md");
    expect(context).not.toContain("/api/transcripts/google_meet/");
  });
});
