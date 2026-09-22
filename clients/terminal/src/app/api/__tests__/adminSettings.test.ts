import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

let cookieJar: Record<string, string> = {};

vi.mock("next/headers", () => ({
  cookies: async () => ({
    get: (name: string) => cookieJar[name] === undefined
      ? undefined
      : { name, value: cookieJar[name] },
  }),
}));

import {
  GET,
  PUT,
} from "../admin/settings/[key]/route";
import { MAX_SETTINGS_REQUEST_BYTES, MAX_SETTINGS_RESPONSE_BYTES } from "../proxyLimits";

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

function json(response: ServerResponse, status: number, body: unknown) {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(body));
}

function readBody(request: IncomingMessage): Promise<string> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
}

function routeRequest(key: string, method: "GET" | "PUT", body?: Record<string, string>) {
  return new NextRequest(`http://terminal.test/api/admin/settings/${key}`, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
}

const context = (key: string) => ({ params: Promise.resolve({ key }) });

beforeEach(() => {
  cookieJar = { "vexa-token": "admin-token" };
  process.env.VEXA_INTERNAL_API_SECRET = "internal-secret";
  delete process.env.VEXA_ADMIN_EMAILS;
});

afterEach(() => {
  delete process.env.VEXA_ADMIN_API_URL;
  delete process.env.VEXA_INTERNAL_API_SECRET;
  vi.restoreAllMocks();
});

describe("admin settings browser boundary", () => {
  it("default-denies unknown settings keys before the privileged admin hop", async () => {
    let requests = 0;
    const upstream = await serve((_request, response) => {
      requests++;
      json(response, 200, {});
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;
    try {
      const result = await GET(routeRequest("secrets", "GET"), context("secrets"));
      expect(result.status).toBe(404);
      expect(requests).toBe(0);
    } finally {
      await upstream.close();
    }
  });

  it("reads the exact non-secret setup state through the admin boundary", async () => {
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      json(response, 200, {
        key: "setup",
        value: { models: "done", transcription: "skipped", completed: "true" },
      });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      const result = await GET(routeRequest("setup", "GET"), context("setup"));
      expect(result.status).toBe(200);
      expect(await result.json()).toEqual({
        key: "setup",
        value: { models: "done", transcription: "skipped", completed: "true" },
      });
    } finally {
      await upstream.close();
    }
  });

  it("rejects an invalid setup-state PUT before the privileged settings hop", async () => {
    let setupWrites = 0;
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      setupWrites++;
      json(response, 200, { key: "setup", value: { models: "finished" } });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      const result = await PUT(
        routeRequest("setup", "PUT", { models: "finished" }),
        context("setup"),
      );
      expect(result.status).toBe(400);
      expect(setupWrites).toBe(0);
    } finally {
      await upstream.close();
    }
  });

  it("persists an exact partial setup-state PUT", async () => {
    let update: Record<string, string> | null = null;
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      void readBody(request).then((raw) => {
        update = JSON.parse(raw) as Record<string, string>;
        json(response, 200, { key: "setup", value: update });
      });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      const result = await PUT(
        routeRequest("setup", "PUT", { models: "done" }),
        context("setup"),
      );
      expect(result.status).toBe(200);
      expect(update).toEqual({ models: "done" });
      expect(await result.json()).toEqual({ key: "setup", value: { models: "done" } });
    } finally {
      await upstream.close();
    }
  });

  it("masks transcription and model secrets on GET", async () => {
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      if (request.url === "/internal/settings/transcription") {
        json(response, 200, {
          key: "transcription",
          value: { url: "http://stt.internal", token: "operator-super-secret" },
        });
        return;
      }
      json(response, 200, {
        key: "models",
        value: { mode: "custom", api_key: "model-super-secret" },
      });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      const transcription = await GET(routeRequest("transcription", "GET"), context("transcription"));
      const transcriptionText = await transcription.text();
      expect(transcriptionText).not.toContain("operator-super-secret");
      expect(JSON.parse(transcriptionText).value.token).toMatch(/^\*{8}/);

      const models = await GET(routeRequest("models", "GET"), context("models"));
      const modelsText = await models.text();
      expect(modelsText).not.toContain("model-super-secret");
      expect(JSON.parse(modelsText).value.api_key).toMatch(/^\*{8}/);
    } finally {
      await upstream.close();
    }
  });

  it.each([
    ["malformed JSON", '{"value":{"token":"operator-super-secret"}'],
    ["non-object JSON", '"operator-super-secret"'],
  ])("fails closed on %s from a secret-bearing settings read", async (_case, upstreamBody) => {
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(upstreamBody);
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      const result = await GET(routeRequest("transcription", "GET"), context("transcription"));
      const body = await result.text();
      expect(result.status).toBe(502);
      expect(body).not.toContain("operator-super-secret");
    } finally {
      await upstream.close();
    }
  });

  it("default-denies unexpected top-level and value fields in a privileged response", async () => {
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      json(response, 200, {
        key: "models",
        future_top_level_secret: "top-secret",
        value: { mode: "custom", future_value_secret: "value-secret" },
      });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;
    try {
      const result = await GET(routeRequest("models", "GET"), context("models"));
      const body = await result.text();
      expect(result.status).toBe(502);
      expect(body).not.toContain("top-secret");
      expect(body).not.toContain("value-secret");
    } finally {
      await upstream.close();
    }
  });

  it("preserves partial PUT and explicit secret-clear semantics while masking read-back", async () => {
    let stored = { url: "http://stt.internal", token: "operator-super-secret" };
    const updates: Array<Record<string, string>> = [];
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      void readBody(request).then((raw) => {
        const update = JSON.parse(raw) as Record<string, string>;
        updates.push(update);
        for (const [field, value] of Object.entries(update)) {
          if (value === "") delete stored[field as keyof typeof stored];
          else stored = { ...stored, [field]: value };
        }
        json(response, 200, { key: "transcription", value: stored });
      });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      const partial = await PUT(
        routeRequest("transcription", "PUT", { url: "http://stt.internal" }),
        context("transcription"),
      );
      expect(updates[0]).toEqual({ url: "http://stt.internal" });
      const partialText = await partial.text();
      expect(partialText).not.toContain("operator-super-secret");
      expect(JSON.parse(partialText).value.token).toMatch(/^\*{8}/);

      const cleared = await PUT(
        routeRequest("transcription", "PUT", { token: "" }),
        context("transcription"),
      );
      expect(updates[1]).toEqual({ token: "" });
      expect((await cleared.json()).value).toEqual({ url: "http://stt.internal" });
    } finally {
      await upstream.close();
    }
  });

  it("refuses an admin-api redirect without replaying the internal secret", async () => {
    let sinkRequests = 0;
    let leakedSecret = "";
    const sink = await serve((request, response) => {
      sinkRequests++;
      leakedSecret = request.headers["x-internal-secret"] as string ?? "";
      json(response, 200, { key: "transcription", value: {} });
    });
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      response.writeHead(307, { Location: `${sink.baseUrl}/internal-secret-sink` });
      response.end();
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      const result = await GET(routeRequest("transcription", "GET"), context("transcription"));
      expect(result.status).toBe(502);
      expect(sinkRequests).toBe(0);
      expect(leakedSecret).toBe("");
    } finally {
      await upstream.close();
      await sink.close();
    }
  });

  it("bounds and validates updates before the privileged settings request", async () => {
    let settingsRequests = 0;
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      settingsRequests++;
      json(response, 200, { key: "models", value: {} });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;
    try {
      const oversized = new NextRequest("http://terminal.test/api/admin/settings/models", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": String(MAX_SETTINGS_REQUEST_BYTES + 1),
        },
        body: "{}",
      });
      let result = await PUT(oversized, context("models"));
      expect(result.status).toBe(413);
      expect(settingsRequests).toBe(0);

      result = await PUT(
        routeRequest("models", "PUT", { arbitrary_secret_field: "secret" }),
        context("models"),
      );
      expect(result.status).toBe(400);
      expect(settingsRequests).toBe(0);

      result = await PUT(
        routeRequest("models", "PUT", { api_key: "********-masked" }),
        context("models"),
      );
      expect(result.status).toBe(400);
      expect(settingsRequests).toBe(0);
    } finally {
      await upstream.close();
    }
  });

  it("bounds success responses and never reflects upstream error bodies", async () => {
    let mode: "oversized" | "error" = "oversized";
    const upstream = await serve((request, response) => {
      if (request.url === "/internal/validate") {
        request.resume();
        request.on("end", () => json(response, 200, {
          user_id: 1,
          email: "admin@example.com",
          is_admin: true,
        }));
        return;
      }
      if (mode === "oversized") {
        response.writeHead(200, {
          "Content-Type": "application/json",
          "Content-Length": String(MAX_SETTINGS_RESPONSE_BYTES + 1),
        });
        response.end("{}");
        return;
      }
      response.writeHead(500, { "Content-Type": "text/plain" });
      response.end("echoed X-Internal-Secret: internal-secret");
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;
    try {
      let result = await GET(routeRequest("models", "GET"), context("models"));
      expect(result.status).toBe(502);

      mode = "error";
      result = await GET(routeRequest("models", "GET"), context("models"));
      const body = await result.text();
      expect(result.status).toBe(500);
      expect(body).not.toContain("internal-secret");
      expect(body).not.toContain("X-Internal-Secret");
    } finally {
      await upstream.close();
    }
  });
});
