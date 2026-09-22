import { afterEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("../proxyAuth", () => ({ resolveApiKey: async () => "user-key" }));

import { GET } from "../meeting/stream/route";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("meeting stream credential boundary", () => {
  it("turns upstream and transport errors into stable local SSE events", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(
      "bad X-API-Key: user-key",
      { status: 500 },
    )));
    let result = await GET(new NextRequest("http://terminal.test/api/meeting/stream"));
    let body = await result.text();
    expect(body).toContain("agent-api stream returned 500");
    expect(body).not.toContain("user-key");
    expect(body).not.toContain("bad X-API-Key");

    vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("connect user-key secret"); }));
    result = await GET(new NextRequest("http://terminal.test/api/meeting/stream"));
    body = await result.text();
    expect(body).toContain("upstream unavailable");
    expect(body).not.toContain("user-key");
    expect(body).not.toContain("connect");
  });
});
