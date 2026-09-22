import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

/** Cookie jar the mocked next/headers writes into, so the test can assert what login set. */
let setCookies: Array<{ name: string; value: string; opts?: unknown }> = [];

vi.mock("next/headers", () => ({
  cookies: async () => ({
    get: () => undefined,
    set: (name: string, value: string, opts?: unknown) => setCookies.push({ name, value, opts }),
    delete: () => {},
  }),
}));

import { POST as login } from "../login/route";

function makeReq(body: unknown): import("next/server").NextRequest {
  return new Request("http://local/api/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }) as unknown as import("next/server").NextRequest;
}

beforeEach(() => {
  setCookies = [];
  process.env.VEXA_ADMIN_API_URL = "http://admin.test";
  process.env.VEXA_ADMIN_API_KEY = "admin-secret";
  process.env.VEXA_DIRECT_LOGIN_ALLOWED_EMAILS = "test-a@b.com,test-new@b.com";
  process.env.NEXTAUTH_URL = "http://localhost:3000";
  process.env.HOST = "127.0.0.1";
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete process.env.VEXA_DIRECT_LOGIN_ALLOWED_EMAILS;
  delete process.env.NEXTAUTH_URL;
  delete process.env.TERMINAL_URL;
  delete process.env.HOST;
  delete process.env.VEXA_TERMINAL_HOST_BIND;
});

describe("/api/auth/login — direct email login against a mocked admin-api", () => {
  it("finds an existing user, mints a token, and sets both cookies", async () => {
    const calls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        calls.push(`${init?.method || "GET"} ${url}`);
        if (url.includes("/admin/capabilities")) return new Response("not found", { status: 404 });
        if (url.includes("/admin/users/email/")) {
          return new Response(JSON.stringify({ id: 42, email: "test-a@b.com", name: "A" }), { status: 200 });
        }
        if (url.includes("/tokens")) {
          return new Response(JSON.stringify({ token: "minted-tok" }), { status: 200 });
        }
        return new Response("nope", { status: 500 });
      }),
    );

    const res = await login(makeReq({ email: "test-a@b.com" }));
    expect(res.status).toBe(200);

    // No create call — user already existed.
    expect(calls.some((c) => c.includes("/admin/users/email/"))).toBe(true);
    expect(calls.some((c) => c.startsWith("POST") && c.endsWith("/admin/users"))).toBe(false);
    expect(calls.some((c) => c.includes("/tokens"))).toBe(true);
    const mint = calls.find((c) => c.includes("/tokens"));
    expect(mint).toContain("scopes=bot%2Ctx%2Cbrowser");
    expect(mint).not.toContain("agent");
    // an EXISTING user is not re-provisioned (eager provisioning fires only on account creation)
    expect(calls.some((c) => c.includes("/agent/workspace/init"))).toBe(false);

    const tok = setCookies.find((c) => c.name === "vexa-token");
    const info = setCookies.find((c) => c.name === "vexa-user-info");
    expect(tok?.value).toBe("minted-tok");
    expect(JSON.parse(info!.value)).toEqual({ email: "test-a@b.com", name: "A" });
    expect((tok?.opts as { httpOnly?: boolean })?.httpOnly).toBe(true);
  });

  it("rejects a non-test email (debug-only path) without calling admin-api", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const res = await login(makeReq({ email: "real@company.com" }));
    expect(res.status).toBe(403);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("creates the user when admin-api returns 404, then mints a token", async () => {
    const calls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        calls.push(`${init?.method || "GET"} ${url}`);
        if (url.includes("/admin/capabilities")) return new Response("not found", { status: 404 });
        if (url.includes("/admin/users/email/")) return new Response("not found", { status: 404 });
        if (init?.method === "POST" && url.endsWith("/admin/users")) {
          return new Response(JSON.stringify({ id: 7, email: "test-new@b.com" }), { status: 201 });
        }
        if (url.includes("/tokens")) return new Response(JSON.stringify({ token: "tok-7" }), { status: 200 });
        return new Response("nope", { status: 500 });
      }),
    );

    const res = await login(makeReq({ email: "test-new@b.com" }));
    expect(res.status).toBe(200);
    expect(calls.some((c) => c.startsWith("POST") && c.endsWith("/admin/users"))).toBe(true);
    expect(setCookies.find((c) => c.name === "vexa-token")?.value).toBe("tok-7");
    // a NEW account eagerly provisions the agent workspace over the gateway (best-effort — a 500 here
    // is swallowed, so sign-in still succeeds above); it authenticates with the freshly minted token
    const provision = calls.find((c) => c.includes("/agent/workspace/init"));
    expect(provision).toBeTruthy();
    expect(provision!.startsWith("POST")).toBe(true);
  });

  it("rejects a malformed email without calling admin-api", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const res = await login(makeReq({ email: "not-an-email" }));
    expect(res.status).toBe(400);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("requests Agent only after identity.v2 is explicitly advertised", async () => {
    const calls: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (url: string, init?: RequestInit) => {
      calls.push(`${init?.method || "GET"} ${url}`);
      if (url.includes("/admin/users/email/")) {
        return new Response(JSON.stringify({ id: 42, email: "test-a@b.com" }), { status: 200 });
      }
      if (url.includes("/admin/capabilities")) {
        return new Response(JSON.stringify({
          contracts: { identity: { versions: ["identity.v1", "identity.v2"], preferred: "identity.v2" } },
        }), { status: 200 });
      }
      if (url.includes("/tokens")) return new Response(JSON.stringify({ token: "v2-token" }), { status: 201 });
      return new Response("nope", { status: 500 });
    }));

    const res = await login(makeReq({ email: "test-a@b.com" }));

    expect(res.status).toBe(200);
    const mint = calls.find((call) => call.includes("/tokens"));
    expect(mint).toContain("scopes=bot%2Ctx%2Cbrowser%2Cagent");
    expect(mint).toContain("contract_version=identity.v2");
  });

  it("does not silently retry v1 after a server advertises v2 and rejects the mint", async () => {
    const calls: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (url: string, init?: RequestInit) => {
      calls.push(`${init?.method || "GET"} ${url}`);
      if (url.includes("/admin/users/email/")) {
        return new Response(JSON.stringify({ id: 42, email: "test-a@b.com" }), { status: 200 });
      }
      if (url.includes("/admin/capabilities")) {
        return new Response(JSON.stringify({
          contracts: { identity: { versions: ["identity.v1", "identity.v2"], preferred: "identity.v2" } },
        }), { status: 200 });
      }
      if (url.includes("/tokens")) return new Response("v2 mint rejected", { status: 422 });
      return new Response("nope", { status: 500 });
    }));

    const res = await login(makeReq({ email: "test-a@b.com" }));

    expect(res.status).toBe(422);
    expect(calls.filter((call) => call.includes("/tokens"))).toHaveLength(1);
  });

  it("is default-off and returns 404 before reading a request body", async () => {
    delete process.env.VEXA_DIRECT_LOGIN_ALLOWED_EMAILS;
    const json = vi.fn(async () => { throw new Error("body must not be read"); });
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    const res = await login({ json } as unknown as import("next/server").NextRequest);

    expect(res.status).toBe(404);
    expect(json).not.toHaveBeenCalled();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("refuses direct login at a public origin before spending admin credentials", async () => {
    process.env.NEXTAUTH_URL = "https://minutes.example.com";
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    const res = await login(makeReq({ email: "test-a@b.com" }));

    expect(res.status).toBe(404);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("uses an exact email allowlist; substring matches such as contest are denied", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    const res = await login(makeReq({ email: "contest@b.com" }));

    expect(res.status).toBe(403);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("rejects missing and oversized request bodies without admin-api egress", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);

    const missing = await login(new Request("http://local/api/auth/login", {
      method: "POST",
    }) as unknown as import("next/server").NextRequest);
    const oversized = await login(new Request("http://local/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Content-Length": String(64 * 1024 + 1) },
      body: "{}",
    }) as unknown as import("next/server").NextRequest);

    expect(missing.status).toBe(400);
    expect(oversized.status).toBe(413);
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});
