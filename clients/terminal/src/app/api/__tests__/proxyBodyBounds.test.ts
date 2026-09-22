import { afterEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("../proxyAuth", () => ({ resolveApiKey: async () => "user-key" }));

import {
  POST,
  GET,
} from "../[...path]/route";
import { readBoundedBytes } from "../boundedBody";
import { MAX_PROXY_REQUEST_BYTES, MAX_PROXY_RESPONSE_BYTES } from "../proxyLimits";

const ctx = (...path: string[]) => ({ params: Promise.resolve({ path }) });

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("catch-all proxy bounded browser edge", () => {
  it("marks transcript JSON responses no-store", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(
      JSON.stringify({ segments: [{ text: "private meeting words" }] }),
      { status: 200, headers: { "Content-Type": "application/json" } },
    )));

    const result = await GET(
      new NextRequest("http://terminal.test/api/transcripts/by-id/42"),
      ctx("transcripts", "by-id", "42"),
    );

    expect(result.status).toBe(200);
    expect(result.headers.get("Cache-Control")).toBe("no-store");
  });

  it("rejects declared oversized ingress before gateway egress", async () => {
    const gateway = vi.fn();
    vi.stubGlobal("fetch", gateway);
    const request = new NextRequest("http://terminal.test/api/meetings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": String(MAX_PROXY_REQUEST_BYTES + 1),
      },
      body: "{}",
    });

    const result = await POST(request, ctx("meetings"));
    expect(result.status).toBe(413);
    expect(gateway).not.toHaveBeenCalled();
  });

  it("cancels a chunked body at the first byte-cap crossing", async () => {
    let pulls = 0;
    let cancelled = false;
    const source = {
      headers: new Headers(),
      body: new ReadableStream<Uint8Array>({
        pull(controller) {
          pulls++;
          controller.enqueue(new Uint8Array([1, 2, 3]));
        },
        cancel() { cancelled = true; },
      }),
    };
    expect(await readBoundedBytes(source, 4)).toBeNull();
    expect(cancelled).toBe(true);
    expect(pulls).toBeLessThan(10);
  });

  it("rejects oversized non-SSE upstream responses before buffering", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("{}", {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Content-Length": String(MAX_PROXY_RESPONSE_BYTES + 1),
      },
    })));
    const result = await GET(
      new NextRequest("http://terminal.test/api/transcripts/latest"),
      ctx("transcripts", "latest"),
    );
    expect(result.status).toBe(502);
    expect(await result.json()).toEqual({ error: "upstream_response_too_large" });
  });

  it("never reflects gateway error bodies or transport exception text", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(
      "bad X-API-Key: user-key",
      { status: 500 },
    )));
    let result = await GET(new NextRequest("http://terminal.test/api/meetings"), ctx("meetings"));
    let body = await result.text();
    expect(result.status).toBe(500);
    expect(body).not.toContain("user-key");
    expect(body).not.toContain("bad X-API-Key");

    vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("connect user-key secret"); }));
    result = await GET(new NextRequest("http://terminal.test/api/meetings"), ctx("meetings"));
    body = await result.text();
    expect(result.status).toBe(502);
    expect(body).not.toContain("user-key");
    expect(body).not.toContain("connect");
  });
});
