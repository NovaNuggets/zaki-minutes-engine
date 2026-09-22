import { describe, expect, it } from "vitest";

import { canonicalMeetingDocPath, canonicalMeetingRow } from "../minutesIdentity";

describe("canonical Minutes row identity", () => {
  it("derives document paths only from bounded numeric database rows", () => {
    expect(canonicalMeetingRow("42")).toBe("42");
    expect(canonicalMeetingDocPath("42")).toBe("kg/entities/meeting/42.md");
    expect(canonicalMeetingDocPath("abc-defg-hij")).toBeNull();
    expect(canonicalMeetingDocPath("../../other-tenant")).toBeNull();
    expect(canonicalMeetingDocPath("9223372036854775808")).toBeNull();
  });
});
