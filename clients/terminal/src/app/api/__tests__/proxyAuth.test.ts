import { afterEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

/** Mutable cookie jar the mocked next/headers reads from. */
let jar = new Map<string, string>();

vi.mock("next/headers", () => ({
  cookies: async () => ({
    get: (name: string) => {
      const value = jar.get(name);
      return value === undefined ? undefined : { name, value };
    },
  }),
}));

import { resolveApiKey } from "../proxyAuth";
import {
  directLoginEmailAllowed,
  directLoginModeError,
  readCookieValue,
  resolveTerminalProxyKey,
  sharedKeyModeError,
} from "../../../proxyAuthPolicy.mjs";
import {
  GET as catchAllGet,
  POST as catchAllPost,
  PUT as catchAllPut,
  PATCH as catchAllPatch,
} from "../[...path]/route";
import { POST as chatPost } from "../chat/route";
import { GET as meetingStreamGet } from "../meeting/stream/route";
import { GET as authMeGet } from "../auth/me/route";
import { GET as workspaceGet, POST as workspacePost } from "../workspace/[...seg]/route";

function makeReq(search = ""): import("next/server").NextRequest {
  return new NextRequest(`http://terminal.test/api/test${search}`);
}

function makeReqM(method: string, body = "", search = ""): import("next/server").NextRequest {
  return new NextRequest(`http://terminal.test/api/test${search}`, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body || undefined,
  });
}

afterEach(() => {
  jar = new Map();
  delete process.env.VEXA_API_KEY;
  delete process.env.VEXA_BOT_API_KEY;
  delete process.env.VEXA_TERMINAL_SHARED_KEY_MODE;
  delete process.env.NEXTAUTH_URL;
  delete process.env.TERMINAL_URL;
  delete process.env.HOST;
  delete process.env.VEXA_TERMINAL_HOST_BIND;
  delete process.env.VEXA_DIRECT_LOGIN_ALLOWED_EMAILS;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("resolveApiKey — browser identity", () => {
  it("prefers the vexa-token cookie", async () => {
    jar.set("vexa-token", "user-token-123");
    process.env.VEXA_API_KEY = "env-key";
    process.env.VEXA_BOT_API_KEY = "bot-key";
    expect(await resolveApiKey()).toBe("user-token-123");
  });

  it("does not turn deployment keys into browser identity by default", async () => {
    process.env.VEXA_API_KEY = "env-key";
    process.env.VEXA_BOT_API_KEY = "bot-key";
    expect(await resolveApiKey()).toBe("");
  });

  it("allows an explicit local self-host shared key", async () => {
    process.env.VEXA_TERMINAL_SHARED_KEY_MODE = "true";
    process.env.VEXA_API_KEY = "self-host-key";
    process.env.NEXTAUTH_URL = "http://localhost:3000";
    process.env.HOST = "127.0.0.1";
    expect(await resolveApiKey()).toBe("self-host-key");
  });

  it("never substitutes the bot service key in explicit shared-key mode", async () => {
    process.env.VEXA_TERMINAL_SHARED_KEY_MODE = "true";
    process.env.VEXA_BOT_API_KEY = "bot-service-key";
    expect(await resolveApiKey()).toBe("");
    expect(sharedKeyModeError(process.env)).toMatch(/VEXA_API_KEY/);
  });

  it("refuses shared-key mode at a public Terminal origin", async () => {
    process.env.VEXA_TERMINAL_SHARED_KEY_MODE = "true";
    process.env.VEXA_API_KEY = "must-not-cross-public-edge";
    process.env.NEXTAUTH_URL = "https://minutes.example.com";
    expect(await resolveApiKey()).toBe("");
  });

  it("surfaces a startup error for shared-key mode on a public origin", () => {
    expect(sharedKeyModeError({
      VEXA_TERMINAL_SHARED_KEY_MODE: "true",
      VEXA_API_KEY: "self-host-key",
      TERMINAL_URL: "https://minutes.example.com",
    })).toMatch(/loopback/i);
  });

  it("refuses privileged shared mode when origin or externally reachable bind is not explicit", () => {
    const base = {
      VEXA_TERMINAL_SHARED_KEY_MODE: "true",
      VEXA_API_KEY: "self-host-key",
    };
    expect(sharedKeyModeError(base)).toMatch(/origin/i);
    expect(sharedKeyModeError({
      ...base,
      NEXTAUTH_URL: "http://localhost:3000",
      HOST: "0.0.0.0",
    })).toMatch(/bind/i);
    expect(sharedKeyModeError({
      ...base,
      NEXTAUTH_URL: "http://localhost:3000",
      HOST: "0.0.0.0",
      VEXA_TERMINAL_HOST_BIND: "127.0.0.1",
    })).toBeNull();
  });
});

describe("WebSocket auth-cookie parsing", () => {
  it("treats malformed percent encoding as absent and rejects it in hosted mode", () => {
    const token = readCookieValue("vexa-token=%E0%A4%A", "vexa-token");
    expect(token).toBeUndefined();
    expect(resolveTerminalProxyKey(token, {})).toBe("");
  });
});

describe("direct login policy", () => {
  it("allows only exact configured emails at a loopback origin", () => {
    const env = {
      VEXA_DIRECT_LOGIN_ALLOWED_EMAILS: "Allowed-Test@Example.com",
      NEXTAUTH_URL: "http://localhost:3000",
      HOST: "127.0.0.1",
    };
    expect(directLoginModeError(env)).toBeNull();
    expect(directLoginEmailAllowed("allowed-test@example.com", env)).toBe(true);
    expect(directLoginEmailAllowed("contest@example.com", env)).toBe(false);
  });

  it("refuses a configured direct-login policy at a public origin", () => {
    expect(directLoginModeError({
      VEXA_DIRECT_LOGIN_ALLOWED_EMAILS: "allowed-test@example.com",
      NEXTAUTH_URL: "https://minutes.example.com",
    })).toMatch(/loopback/i);
  });

  it("refuses direct login when origin or externally reachable bind is not explicit", () => {
    const base = { VEXA_DIRECT_LOGIN_ALLOWED_EMAILS: "allowed-test@example.com" };
    expect(directLoginModeError(base)).toMatch(/origin/i);
    expect(directLoginModeError({
      ...base,
      NEXTAUTH_URL: "http://localhost:3000",
      HOST: "0.0.0.0",
    })).toMatch(/bind/i);
  });
});

describe("cookie-required Terminal proxies", () => {
  it("rejects cookie-less chat before reading or forwarding the prompt", async () => {
    process.env.VEXA_API_KEY = "must-not-be-browser-auth";
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const req = {
      signal: new AbortController().signal,
      headers: new Headers({ "Content-Type": "application/json" }),
      text: vi.fn(async () => { throw new Error("body must not be read before auth"); }),
    } as unknown as import("next/server").NextRequest;

    const response = await chatPost(req);

    expect(response.status).toBe(401);
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(req.text).not.toHaveBeenCalled();
  });

  it("rejects an oversized authenticated chat prompt before gateway egress", async () => {
    jar.set("vexa-token", "user-token");
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const response = await chatPost(new NextRequest("http://terminal.test/api/chat", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": String(512 * 1024 + 1),
      },
      body: "{}",
    }));

    expect(await response.text()).toContain("Chat request is too large");
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("cancels and never reflects a chat gateway error body", async () => {
    jar.set("vexa-token", "user-token");
    let cancelled = false;
    vi.stubGlobal("fetch", vi.fn(async () => new Response(new ReadableStream({
      pull(controller) {
        controller.enqueue(new TextEncoder().encode("secret prompt and upstream credential"));
        controller.close();
      },
      cancel() { cancelled = true; },
    }), { status: 502 })));

    const response = await chatPost(new NextRequest("http://terminal.test/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt: "hello" }),
    }));
    const body = await response.text();

    expect(cancelled).toBe(true);
    expect(body).toContain("Agent request failed");
    expect(body).not.toContain("secret prompt");
    expect(body).not.toContain("credential");
  });

  it("marks streamed model output no-store", async () => {
    jar.set("vexa-token", "user-token");
    vi.stubGlobal("fetch", vi.fn(async () => new Response(
      "data: {\"type\":\"message\",\"text\":\"private answer\"}\n\n",
      { status: 200, headers: { "Content-Type": "text/event-stream" } },
    )));

    const response = await chatPost(new NextRequest("http://terminal.test/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt: "hello" }),
    }));

    expect(response.headers.get("Cache-Control")).toBe("no-store");
  });

  it("uses a stable chat transport log without reflecting thrown details", async () => {
    jar.set("vexa-token", "user-token");
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.stubGlobal("fetch", vi.fn(async () => {
      throw new Error("private gateway URL and user token");
    }));

    const response = await chatPost(new NextRequest("http://terminal.test/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt: "hello" }),
    }));

    expect(await response.text()).not.toContain("private gateway");
    expect(log.mock.calls).toEqual([["[terminal-api] chat proxy failed"]]);
  });

  it("rejects a cookie-less workspace read without contacting the gateway", async () => {
    process.env.VEXA_API_KEY = "must-not-be-browser-auth";
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    const response = await workspaceGet(
      new NextRequest("http://terminal.test/api/workspace/tree"),
      { params: Promise.resolve({ seg: ["tree"] }) },
    );

    expect(response.status).toBe(401);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("rejects a cookie-less workspace write before touching its headers or body", async () => {
    const headersGet = vi.fn(() => "application/json");
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const req = {
      nextUrl: { search: "" },
      headers: { get: headersGet },
      body: "must-not-be-forwarded",
    } as unknown as import("next/server").NextRequest;

    const response = await workspacePost(req, {
      params: Promise.resolve({ seg: ["file"] }),
    });

    expect(response.status).toBe(401);
    expect(headersGet).not.toHaveBeenCalled();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("rejects an oversized authenticated workspace write before gateway egress", async () => {
    jar.set("vexa-token", "user-token");
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const req = new NextRequest("http://terminal.test/api/workspace/upload", {
      method: "POST",
      headers: {
        "Content-Type": "multipart/form-data; boundary=test",
        "Content-Length": String(32 * 1024 * 1024 + 1),
      },
      body: "--test--\r\n",
    });

    const response = await workspacePost(req, { params: Promise.resolve({ seg: ["upload"] }) });

    expect(response.status).toBe(413);
    expect(await response.json()).toEqual({ error: "request_too_large" });
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("cancels oversized and non-2xx workspace responses without reflecting them", async () => {
    jar.set("vexa-token", "user-token");
    let cancelled = 0;
    let mode: "oversized" | "error" = "oversized";
    vi.stubGlobal("fetch", vi.fn(async () => {
      const body = new ReadableStream({
        pull(controller) {
          controller.enqueue(new TextEncoder().encode("private workspace contents and credential"));
          controller.close();
        },
        cancel() { cancelled += 1; },
      });
      return new Response(body, mode === "oversized"
        ? { status: 200, headers: { "Content-Type": "application/json", "Content-Length": String(32 * 1024 * 1024 + 1) } }
        : { status: 502, headers: { "Content-Type": "text/plain" } });
    }));

    const oversized = await workspaceGet(
      new NextRequest("http://terminal.test/api/workspace/tree"),
      { params: Promise.resolve({ seg: ["tree"] }) },
    );
    mode = "error";
    const upstreamError = await workspaceGet(
      new NextRequest("http://terminal.test/api/workspace/tree"),
      { params: Promise.resolve({ seg: ["tree"] }) },
    );

    expect(oversized.status).toBe(502);
    expect(await oversized.json()).toEqual({ error: "upstream_response_too_large" });
    expect(upstreamError.status).toBe(502);
    expect(await upstreamError.json()).toEqual({ error: "upstream_error", status: 502 });
    expect(cancelled).toBe(2);
  });

  it("workspace transport errors use stable browser and log messages", async () => {
    jar.set("vexa-token", "user-token");
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("private gateway URL and token"); }));

    const response = await workspaceGet(
      new NextRequest("http://terminal.test/api/workspace/tree"),
      { params: Promise.resolve({ seg: ["tree"] }) },
    );

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({ error: "upstream_unavailable" });
    expect(log.mock.calls).toEqual([["[terminal-api] workspace read proxy failed"]]);
  });

  it("rejects a cookie-less catch-all write before buffering or forwarding it", async () => {
    process.env.VEXA_BOT_API_KEY = "must-not-be-browser-auth";
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const headersGet = vi.fn(() => "application/json");
    const response = await catchAllPost(
      {
        method: "POST",
        nextUrl: { search: "" },
        headers: { get: headersGet },
        body: "must-not-be-buffered",
      } as unknown as import("next/server").NextRequest,
      { params: Promise.resolve({ path: ["meetings"] }) },
    );

    expect(response.status).toBe(401);
    expect(headersGet).not.toHaveBeenCalled();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("rejects a cookie-less meeting stream before opening an upstream stream", async () => {
    process.env.VEXA_API_KEY = "must-not-be-browser-auth";
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    const response = await meetingStreamGet(
      new NextRequest("http://terminal.test/api/meeting/stream?meeting_id=7"),
    );

    expect(response.status).toBe(401);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("lets the explicit loopback self-host key pass the UI login gate", async () => {
    process.env.VEXA_TERMINAL_SHARED_KEY_MODE = "true";
    process.env.VEXA_API_KEY = "self-host-key";
    process.env.NEXTAUTH_URL = "http://localhost:3000";
    process.env.HOST = "127.0.0.1";

    const response = await authMeGet();

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ authenticated: true, shared: true });
  });
});

describe("catch-all proxy — meetings domain forwards the cookie token as X-API-Key", () => {
  it("injects the cookie token on a /bots call", async () => {
    jar.set("vexa-token", "cookie-tok");
    const seen: { url?: string; key?: string } = {};
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        seen.url = url;
        seen.key = (init?.headers as Record<string, string>)?.["X-API-Key"];
        return new Response("{}", { status: 200, headers: { "Content-Type": "application/json" } });
      }),
    );

    await catchAllGet(makeReq(), { params: Promise.resolve({ path: ["bots"] }) });

    expect(seen.url).toContain("/bots");
    expect(seen.key).toBe("cookie-tok");
  });

});

describe("catch-all proxy — PATCH and PUT verbs forward (regression: handler exported only GET/POST/DELETE → 405)", () => {
  it("forwards PATCH to the agent domain, body intact (routine enable/disable)", async () => {
    jar.set("vexa-token", "tok");
    const seen: { url?: string; method?: string; body?: unknown } = {};
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        seen.url = url;
        seen.method = init?.method;
        seen.body = init?.body;
        return new Response("{}", { status: 200, headers: { "Content-Type": "application/json" } });
      }),
    );

    await catchAllPatch(makeReqM("PATCH", JSON.stringify({ enabled: false })), {
      params: Promise.resolve({ path: ["routines", "daily", "enabled"] }),
    });

    expect(seen.method).toBe("PATCH");
    expect(seen.url).toContain("/agent/routines/daily/enabled");
    expect(seen.body).toBe(JSON.stringify({ enabled: false }));
  });

  it("forwards PUT to the meetings domain (schedule/cancel intent)", async () => {
    jar.set("vexa-token", "tok");
    const seen: { url?: string; method?: string } = {};
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        seen.url = url;
        seen.method = init?.method;
        return new Response("{}", { status: 200, headers: { "Content-Type": "application/json" } });
      }),
    );

    await catchAllPut(makeReqM("PUT", JSON.stringify({ intent: "scheduled" })), {
      params: Promise.resolve({ path: ["meetings", "google_meet", "abc", "intent"] }),
    });

    expect(seen.method).toBe("PUT");
    expect(seen.url).toContain("/meetings/google_meet/abc/intent");
  });
});
