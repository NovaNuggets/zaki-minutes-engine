import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { describe, expect, it } from "vitest";

import { credentialedFetch } from "../credentialedFetch";

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

describe("credentialedFetch", () => {
  it("keeps gateway X-API-Key/body on the direct origin and refuses redirect replay", async () => {
    const redirected: Array<{ key: string; bytes: number }> = [];
    const sink = await serve((request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        redirected.push({
          key: request.headers["x-api-key"] as string ?? "",
          bytes: Buffer.concat(chunks).length,
        });
        response.writeHead(200);
        response.end("redirected");
      });
    });
    const redirecting = await serve((request, response) => {
      request.resume();
      request.on("end", () => {
        response.writeHead(307, { Location: `${sink.baseUrl}/api-key-sink` });
        response.end();
      });
    });

    try {
      await expect(credentialedFetch(`${redirecting.baseUrl}/gateway`, {
        method: "POST",
        headers: { "X-API-Key": "user-secret" },
        body: "private request",
      })).rejects.toThrow();
      expect(redirected).toEqual([]);
    } finally {
      await redirecting.close();
      await sink.close();
    }

    const directRequests: Array<{ key: string; body: string }> = [];
    const direct = await serve((request, response) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        directRequests.push({
          key: request.headers["x-api-key"] as string ?? "",
          body: Buffer.concat(chunks).toString("utf8"),
        });
        response.writeHead(200);
        response.end("ok");
      });
    });
    try {
      const response = await credentialedFetch(`${direct.baseUrl}/gateway`, {
        method: "POST",
        headers: { "X-API-Key": "user-secret" },
        body: "private request",
      });
      expect(response.status).toBe(200);
      expect(directRequests).toEqual([{ key: "user-secret", body: "private request" }]);
    } finally {
      await direct.close();
    }
  });
});
