import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  createUser,
  findUserByEmail,
  getUserBotContext,
  instanceHasAdmin,
  MAX_ADMIN_REQUEST_BYTES,
  MAX_ADMIN_RESPONSE_BYTES,
  validateAuthToken,
} from "../adminApi";

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

beforeEach(() => {
  process.env.VEXA_ADMIN_API_KEY = "admin-api-secret";
  process.env.VEXA_INTERNAL_API_SECRET = "internal-secret";
  delete process.env.VEXA_ADMIN_EMAILS;
});

afterEach(() => {
  delete process.env.VEXA_ADMIN_API_URL;
  delete process.env.VEXA_ADMIN_API_KEY;
  delete process.env.VEXA_INTERNAL_API_SECRET;
  delete process.env.VEXA_ADMIN_EMAILS;
});

describe("adminApi credential egress", () => {
  it("refuses redirects across admin-key and internal-secret helpers without hitting the sink", async () => {
    const sinkRequests: Array<{ internal: string; admin: string; bytes: number }> = [];
    const sink = await serve((request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        sinkRequests.push({
          internal: request.headers["x-internal-secret"] as string ?? "",
          admin: request.headers["x-admin-api-key"] as string ?? "",
          bytes: Buffer.concat(chunks).length,
        });
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end("{}");
      });
    });
    const redirecting = await serve((request, response) => {
      request.resume();
      request.on("end", () => {
        response.writeHead(307, { Location: `${sink.baseUrl}/credential-sink` });
        response.end();
      });
    });
    process.env.VEXA_ADMIN_API_URL = redirecting.baseUrl;

    try {
      const validated = await validateAuthToken("user-token");
      const instance = await instanceHasAdmin();
      const user = await findUserByEmail("person@example.com");

      expect(validated.ok).toBe(false);
      expect(instance).toBe(true); // fail-safe: a refused instance probe never opens bootstrap
      expect(user.ok).toBe(false);
      expect(sinkRequests).toEqual([]);
    } finally {
      await redirecting.close();
      await sink.close();
    }
  });

  it("bounds authenticated requests and success responses", async () => {
    let requests = 0;
    const upstream = await serve((request, response) => {
      requests++;
      request.resume();
      request.on("end", () => {
        response.writeHead(200, {
          "Content-Type": "application/json",
          "Content-Length": String(MAX_ADMIN_RESPONSE_BYTES + 1),
        });
        response.end("{}");
      });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      const oversizedRequest = await createUser("x".repeat(MAX_ADMIN_REQUEST_BYTES + 1));
      expect(oversizedRequest.status).toBe(413);
      expect(requests).toBe(0);

      const validated = await validateAuthToken("user-token");
      const user = await findUserByEmail("person@example.com");
      expect(validated.ok).toBe(false);
      if (!validated.ok) expect(validated.status).toBe(502);
      expect(user.ok).toBe(false);
      expect(user.status).toBe(502);
      expect(requests).toBe(2);
    } finally {
      await upstream.close();
    }
  });

  it("never reflects authenticated upstream error bodies", async () => {
    const upstream = await serve((request, response) => {
      request.resume();
      request.on("end", () => {
        response.writeHead(500, { "Content-Type": "text/plain" });
        response.end("echoed X-Admin-API-Key admin-api-secret X-Internal-Secret internal-secret");
      });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      const validated = await validateAuthToken("user-token");
      const user = await findUserByEmail("person@example.com");
      expect(JSON.stringify(validated)).not.toContain("internal-secret");
      expect(JSON.stringify(validated)).not.toContain("X-Internal-Secret");
      expect(JSON.stringify(user)).not.toContain("admin-api-secret");
      expect(JSON.stringify(user)).not.toContain("X-Admin-API-Key");
    } finally {
      await upstream.close();
    }
  });

  it("fails closed on malformed identity and bot-context shapes", async () => {
    let identity: unknown = {
      user_id: { forged: 7 },
      email: "person@example.com",
      is_admin: false,
    };
    let context: unknown = { transcription: { url: 7, token: "secret" } };
    const upstream = await serve((request, response) => {
      request.resume();
      request.on("end", () => {
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify(
          request.url === "/internal/validate" ? identity : context,
        ));
      });
    });
    process.env.VEXA_ADMIN_API_URL = upstream.baseUrl;

    try {
      let validated = await validateAuthToken("user-token");
      expect(validated.ok).toBe(false);
      if (!validated.ok) expect(validated.status).toBe(502);

      identity = { user_id: 7, email: "x".repeat(321), is_admin: "yes" };
      validated = await validateAuthToken("user-token");
      expect(validated.ok).toBe(false);

      let botContext = await getUserBotContext(7);
      expect(botContext.ok).toBe(false);
      expect(botContext.status).toBe(502);

      context = { transcription: { url: "https://stt.example", token: "x".repeat(8193) } };
      botContext = await getUserBotContext(7);
      expect(botContext.ok).toBe(false);

      context = { transcription: { blocked: "true" } };
      botContext = await getUserBotContext(7);
      expect(botContext.ok).toBe(false);
    } finally {
      await upstream.close();
    }
  });
});
