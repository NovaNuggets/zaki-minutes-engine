import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

let cookieJar: Record<string, string> = {};

vi.mock("next/headers", () => ({
  cookies: async () => ({
    get: (name: string) => cookieJar[name] === undefined
      ? undefined
      : { name, value: cookieJar[name] },
  }),
}));

import { POST } from "../stt/route";
import {
  MAX_STT_REQUEST_BYTES,
  MAX_STT_RESPONSE_BYTES,
  readBoundedBody,
  validSttResponse,
} from "../stt/sttSafety";

async function serve(handler: (request: IncomingMessage, response: ServerResponse) => void) {
  const server = createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return {
    baseUrl: `http://127.0.0.1:${port}`,
    close: () => new Promise<void>((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    }),
  };
}

function validAdmin(transcription?: { url: string; token?: string }) {
  return serve((incoming, response) => {
    incoming.resume();
    incoming.on("end", () => {
      response.writeHead(200, { "Content-Type": "application/json" });
      if (incoming.url === "/internal/validate") {
        response.end(JSON.stringify({
          user_id: 7,
          email: "person@example.com",
          is_admin: false,
        }));
        return;
      }
      response.end(JSON.stringify(transcription ? { transcription } : {}));
    });
  });
}

function request() {
  return new Request("http://terminal.test/api/stt", {
    method: "POST",
    headers: { "Content-Type": "audio/wav" },
    body: new Uint8Array(128).buffer,
  });
}

beforeEach(() => {
  cookieJar = { "vexa-token": "user-key" };
  process.env.VEXA_INTERNAL_API_SECRET = "internal-secret";
});

afterEach(() => {
  delete process.env.VEXA_API_KEY;
  delete process.env.VEXA_BOT_API_KEY;
  delete process.env.VEXA_ADMIN_API_URL;
  delete process.env.VEXA_INTERNAL_API_SECRET;
  delete process.env.TRANSCRIPTION_SERVICE_URL;
  delete process.env.TRANSCRIPTION_SERVICE_TOKEN;
  delete process.env.VEXA_TERMINAL_SHARED_KEY_MODE;
  delete process.env.NEXTAUTH_URL;
  delete process.env.HOST;
  delete process.env.VEXA_TERMINAL_HOST_BIND;
});

describe("POST /api/stt egress", () => {
  it("bounds streamed bodies and cancels as soon as the limit is crossed", async () => {
    let pulls = 0;
    let cancelled = false;
    const oversized = new ReadableStream<Uint8Array>({
      pull(controller) {
        pulls++;
        controller.enqueue(new Uint8Array([1, 2, 3]));
      },
      cancel() {
        cancelled = true;
      },
    });
    const rejected = await readBoundedBody({
      headers: new Headers(),
      body: oversized,
    } as Request, 4);
    expect(rejected).toBeNull();
    expect(pulls).toBe(2);
    expect(cancelled).toBe(true);

    const accepted = await readBoundedBody(new Request("http://terminal.test/body", {
      method: "POST",
      body: new Uint8Array([1, 2, 3, 4]).buffer,
    }), 4);
    expect(Array.from(new Uint8Array(accepted!))).toEqual([1, 2, 3, 4]);
  });

  it("fast-rejects an oversized Content-Length before reading or calling STT", async () => {
    let sttRequests = 0;
    const stt = await serve((incoming, response) => {
      sttRequests++;
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ text: "must not run", segments: [] }));
    });
    const admin = await validAdmin();
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "operator-secret";
    const oversized = new Request("http://terminal.test/api/stt", {
      method: "POST",
      headers: {
        "Content-Type": "audio/wav",
        "Content-Length": String(MAX_STT_REQUEST_BYTES + 1),
      },
      body: new Uint8Array(128).buffer,
    });

    try {
      const result = await POST(oversized);
      expect(result.status).toBe(413);
      expect(sttRequests).toBe(0);
    } finally {
      await admin.close();
      await stt.close();
    }
  });

  it("does not treat deployment fallback keys as browser authentication when the cookie is absent", async () => {
    let adminRequests = 0;
    let sttRequests = 0;
    const admin = await serve((incoming, response) => {
      adminRequests++;
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ user_id: 7, email: "operator@example.com" }));
      });
    });
    const stt = await serve((incoming, response) => {
      sttRequests++;
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "must not run", segments: [] }));
      });
    });
    cookieJar = {};
    process.env.VEXA_API_KEY = "deployment-fallback-key";
    process.env.VEXA_BOT_API_KEY = "legacy-deployment-key";
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "operator-secret";

    try {
      const result = await POST(request());
      expect(result.status).toBe(401);
      expect(result.headers.get("Cache-Control")).toBe("no-store");
      expect(adminRequests).toBe(0);
      expect(sttRequests).toBe(0);
    } finally {
      await admin.close();
      await stt.close();
    }
  });

  it("validates the explicit loopback shared user key before spending STT", async () => {
    cookieJar = {};
    process.env.VEXA_TERMINAL_SHARED_KEY_MODE = "true";
    process.env.VEXA_API_KEY = "shared-self-host-token";
    process.env.NEXTAUTH_URL = "http://localhost:3001";
    process.env.HOST = "0.0.0.0";
    process.env.VEXA_TERMINAL_HOST_BIND = "127.0.0.1";
    const admin = await validAdmin();
    const stt = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "shared dictation", segments: [] }));
      });
    });
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;

    try {
      const result = await POST(request());
      expect(result.status).toBe(200);
      expect((await result.json()).text).toBe("shared dictation");
    } finally {
      await admin.close();
      await stt.close();
    }
  });

  it("rejects a forged auth cookie before spending the operator STT credential", async () => {
    let sttRequests = 0;
    const stt = await serve((incoming, response) => {
      sttRequests++;
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "must not run", segments: [] }));
      });
    });
    const admin = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(401, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ detail: "invalid token" }));
      });
    });
    cookieJar = { "vexa-token": "forged-token" };
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "operator-secret";

    try {
      const result = await POST(request());
      expect(result.status).toBe(401);
      expect(sttRequests).toBe(0);
    } finally {
      await admin.close();
      await stt.close();
    }
  });

  it("rejects redirects without replaying audio or the operator token", async () => {
    let sinkRequests = 0;
    let sinkBytes = 0;
    let sinkAuthorization = "";
    const sink = await serve((incoming, response) => {
      sinkRequests++;
      sinkAuthorization = incoming.headers.authorization ?? "";
      incoming.on("data", (chunk: Buffer) => { sinkBytes += chunk.length; });
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "redirected", segments: [] }));
      });
    });
    const redirecting = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(307, { Location: `${sink.baseUrl}/credential-sink` });
        response.end();
      });
    });
    const admin = await validAdmin();
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = redirecting.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "operator-secret";

    try {
      const result = await POST(request());
      expect(result.status).toBe(502);
      expect(sinkRequests).toBe(0);
      expect(sinkBytes).toBe(0);
      expect(sinkAuthorization).toBe("");
    } finally {
      await admin.close();
      await redirecting.close();
      await sink.close();
    }
  });

  it("uses the effective user STT tier atomically without inheriting the env operator token", async () => {
    let userAuthorization = "not-called";
    let userRequests = 0;
    let envRequests = 0;
    const userStt = await serve((incoming, response) => {
      userRequests++;
      userAuthorization = incoming.headers.authorization ?? "";
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "user tier", segments: [] }));
      });
    });
    const envStt = await serve((incoming, response) => {
      envRequests++;
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "wrong tier", segments: [] }));
      });
    });
    const admin = await validAdmin({ url: userStt.baseUrl });
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = envStt.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "env-operator-secret";

    try {
      const result = await POST(request());
      expect(result.status).toBe(200);
      expect((await result.json()).text).toBe("user tier");
      expect(userRequests).toBe(1);
      expect(userAuthorization).toBe("");
      expect(envRequests).toBe(0);
    } finally {
      await admin.close();
      await userStt.close();
      await envStt.close();
    }
  });

  it("uses the effective platform STT tier returned by bot-context", async () => {
    let platformAuthorization = "";
    let envRequests = 0;
    const platformStt = await serve((incoming, response) => {
      platformAuthorization = incoming.headers.authorization ?? "";
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "platform tier", segments: [] }));
      });
    });
    const envStt = await serve((incoming, response) => {
      envRequests++;
      response.writeHead(500);
      response.end();
    });
    const admin = await validAdmin({
      url: platformStt.baseUrl,
      token: "platform-token",
    });
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = envStt.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "env-operator-secret";

    try {
      const result = await POST(request());
      expect(result.status).toBe(200);
      expect((await result.json()).text).toBe("platform tier");
      expect(platformAuthorization).toBe("Bearer platform-token");
      expect(envRequests).toBe(0);
    } finally {
      await admin.close();
      await platformStt.close();
      await envStt.close();
    }
  });

  it("refuses a blocked personal STT tier without spending the env operator credential", async () => {
    let envRequests = 0;
    const envStt = await serve((incoming, response) => {
      envRequests++;
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "wrong tier", segments: [] }));
      });
    });
    const admin = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        if (incoming.url === "/internal/validate") {
          response.end(JSON.stringify({ user_id: 7, email: "person@example.com" }));
          return;
        }
        response.end(JSON.stringify({
          transcription: {
            blocked: true,
            config_status: "blocked",
            validation_error: "Personal transcription endpoint is no longer operator-approved.",
          },
        }));
      });
    });
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = envStt.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "env-operator-secret";

    try {
      const result = await POST(request());
      expect(result.status).toBe(503);
      expect(envRequests).toBe(0);
      expect(await result.json()).toEqual({ error: "Personal transcription configuration is blocked" });
    } finally {
      await admin.close();
      await envStt.close();
    }
  });

  it("fails closed when auth validation redirects without leaking the internal secret or spending STT", async () => {
    let authSinkRequests = 0;
    let authSinkSecret = "";
    let sttRequests = 0;
    const authSink = await serve((incoming, response) => {
      authSinkRequests++;
      authSinkSecret = incoming.headers["x-internal-secret"] as string ?? "";
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ user_id: 7, email: "person@example.com" }));
    });
    const redirectingAdmin = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(307, { Location: `${authSink.baseUrl}/internal-secret-sink` });
        response.end();
      });
    });
    const stt = await serve((incoming, response) => {
      sttRequests++;
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "must not run", segments: [] }));
      });
    });
    process.env.VEXA_ADMIN_API_URL = redirectingAdmin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "operator-secret";

    try {
      const result = await POST(request());
      expect(result.status).toBe(401);
      expect(authSinkRequests).toBe(0);
      expect(authSinkSecret).toBe("");
      expect(sttRequests).toBe(0);
    } finally {
      await redirectingAdmin.close();
      await authSink.close();
      await stt.close();
    }
  });

  it("refuses a bot-context redirect without leaking the internal secret or spending STT", async () => {
    let contextSinkRequests = 0;
    let contextSinkSecret = "";
    let sttRequests = 0;
    const contextSink = await serve((incoming, response) => {
      contextSinkRequests++;
      contextSinkSecret = incoming.headers["x-internal-secret"] as string ?? "";
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ transcription: {} }));
    });
    const admin = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        if (incoming.url === "/internal/validate") {
          response.writeHead(200, { "Content-Type": "application/json" });
          response.end(JSON.stringify({ user_id: 7, email: "person@example.com" }));
          return;
        }
        response.writeHead(307, { Location: `${contextSink.baseUrl}/internal-secret-sink` });
        response.end();
      });
    });
    const stt = await serve((incoming, response) => {
      sttRequests++;
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "must not run", segments: [] }));
      });
    });
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "operator-secret";

    try {
      const result = await POST(request());
      expect(result.status).toBe(503);
      expect(contextSinkRequests).toBe(0);
      expect(contextSinkSecret).toBe("");
      expect(sttRequests).toBe(0);
    } finally {
      await admin.close();
      await contextSink.close();
      await stt.close();
    }
  });

  it("keeps direct operator HTTP working and canonicalizes a /v1 service URL once", async () => {
    let path = "";
    let authorization = "";
    let bytes = 0;
    const direct = await serve((incoming, response) => {
      path = incoming.url ?? "";
      authorization = incoming.headers.authorization ?? "";
      incoming.on("data", (chunk: Buffer) => { bytes += chunk.length; });
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ text: "hello", segments: [] }));
      });
    });
    const admin = await validAdmin();
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = `${direct.baseUrl}/v1/`;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "operator-secret";

    try {
      const result = await POST(request());
      expect(result.status).toBe(200);
      expect(result.headers.get("Cache-Control")).toBe("no-store");
      expect(await result.json()).toEqual({ text: "hello", words: [] });
      expect(path).toBe("/v1/audio/transcriptions");
      expect(authorization).toBe("Bearer operator-secret");
      expect(bytes).toBeGreaterThan(0);
    } finally {
      await admin.close();
      await direct.close();
    }
  });

  it("fast-rejects a declared oversized STT response without buffering it", async () => {
    const stt = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, {
          "Content-Type": "application/json",
          "Content-Length": String(MAX_STT_RESPONSE_BYTES + 1),
        });
        response.end("{}");
      });
    });
    const admin = await validAdmin();
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;

    try {
      const result = await POST(request());
      expect(result.status).toBe(502);
      expect(await result.json()).toEqual({ error: "Transcription returned an oversized response" });
    } finally {
      await admin.close();
      await stt.close();
    }
  });

  it("stops a chunked STT response at the byte cap", async () => {
    const stt = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.write(Buffer.alloc(MAX_STT_RESPONSE_BYTES, 0x20));
        response.end(Buffer.from("x"));
      });
    });
    const admin = await validAdmin();
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;

    try {
      const result = await POST(request());
      expect(result.status).toBe(502);
      expect(await result.json()).toEqual({ error: "Transcription returned an oversized response" });
    } finally {
      await admin.close();
      await stt.close();
    }
  });

  it("rejects attacker-controlled response fanout and malformed timestamp values", async () => {
    expect(validSttResponse({
      text: "bounded",
      segments: Array.from({ length: 10_001 }, () => ({ words: [] })),
    })).toBe(false);
    expect(validSttResponse({
      text: "bounded",
      segments: [{ words: [{ word: "hello", start: Number.NaN, end: 1 }] }],
    })).toBe(false);

    const stt = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({
          text: "bounded",
          segments: [{ words: [{ word: "hello", start: "not-a-number", end: 1 }] }],
        }));
      });
    });
    const admin = await validAdmin();
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;

    try {
      const result = await POST(request());
      expect(result.status).toBe(502);
      expect(await result.json()).toEqual({ error: "Transcription returned an invalid response" });
    } finally {
      await admin.close();
      await stt.close();
    }
  });

  it("never reflects an untrusted upstream error body that echoes the STT token", async () => {
    const stt = await serve((incoming, response) => {
      incoming.resume();
      incoming.on("end", () => {
        response.writeHead(401, { "Content-Type": "text/plain" });
        response.end("bad Authorization: Bearer operator-secret");
      });
    });
    const admin = await validAdmin();
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = stt.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_TOKEN = "operator-secret";

    try {
      const result = await POST(request());
      const body = await result.text();
      expect(result.status).toBe(502);
      expect(body).not.toContain("operator-secret");
      expect(body).not.toContain("bad Authorization");
    } finally {
      await admin.close();
      await stt.close();
    }
  });

  it("cancels an untrusted STT error body instead of leaving it unread", async () => {
    const admin = await validAdmin();
    process.env.VEXA_ADMIN_API_URL = admin.baseUrl;
    process.env.TRANSCRIPTION_SERVICE_URL = "http://stt.invalid";
    const originalFetch = globalThis.fetch;
    let cancelled = false;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).startsWith(admin.baseUrl)) return originalFetch(input, init);
      return new Response(new ReadableStream({
        pull(controller) {
          controller.enqueue(new TextEncoder().encode("private echoed STT credential"));
        },
        cancel() { cancelled = true; },
      }), { status: 500 });
    }));

    try {
      const result = await POST(request());
      expect(result.status).toBe(502);
      expect(cancelled).toBe(true);
    } finally {
      await admin.close();
    }
  });
});
