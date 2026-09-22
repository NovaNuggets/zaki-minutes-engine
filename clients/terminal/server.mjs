// Custom Next.js server with a real server-side WebSocket proxy for `/ws`.
//
// Why this exists: Next.js `rewrites()` proxy HTTP only — they do NOT proxy the
// WebSocket `upgrade` handshake. So a browser opening same-origin `wss://host/ws`
// never reaches the gateway. This server intercepts the HTTP `upgrade` event for
// path `/ws`, opens a *server-side* socket to the gateway with the `x-api-key`
// header (key stays server-side, never in any client-visible URL), and pipes
// frames bidirectionally. The browser connects KEYLESS to same-origin `/ws`.
//
// The key injected here is the SAME per-user key the REST proxy forwards
// (src/app/api/proxyAuth.ts): the logged-in user's APIToken from the `vexa-token`
// cookie, or the explicit loopback-only VEXA_API_KEY shared mode. This MUST match the REST
// side — the gateway auto-subscribes the socket to `u:{user_id}:meetings` from this
// key, so a mismatched key would deliver another user's live meeting.status frames
// (or none), freezing the client's meeting list at its last REST snapshot.
import { createServer } from "node:http";
import nextEnv from "@next/env";
import next from "next";
import { WebSocketServer, WebSocket } from "ws";
import {
  directLoginModeError,
  readCookieValue,
  resolveTerminalProxyKey,
  sharedKeyModeError,
} from "./src/proxyAuthPolicy.mjs";
import {
  PendingFrameQueue,
  WS_PROXY_LIMITS,
  armConnectTimeout,
  socketCanAcceptFrame,
} from "./src/wsProxySafety.mjs";

const dev = process.env.NODE_ENV !== "production";
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd(), dev);

const sharedModeError = sharedKeyModeError(process.env);
if (sharedModeError) {
  throw new Error(`[terminal-auth] ${sharedModeError}`);
}
const directLoginError = directLoginModeError(process.env);
if (directLoginError) {
  throw new Error(`[terminal-auth] ${directLoginError}`);
}

const port = parseInt(process.env.PORT || "3000", 10);
const hostname = process.env.HOST || "0.0.0.0";

const GATEWAY_URL = (process.env.GATEWAY_URL || "ws://127.0.0.1:18056")
  .replace(/\/$/, "")
  .replace(/^http/, "ws");

// The httpOnly cookie carrying the logged-in user's APIToken (set by /api/auth/login).
// Mirrors AUTH_COOKIE in src/app/api/auth/adminApi.ts.
const AUTH_COOKIE = process.env.VEXA_AUTH_COOKIE_NAME || "vexa-token";

/** Resolve the x-api-key to send upstream on the `/ws` upgrade, PER REQUEST. The gateway resolves the
 *  user_id from this key at connect and auto-subscribes the socket to `u:{user_id}:meetings` — so it MUST
 *  be the same per-user key the REST proxy forwards (src/app/api/proxyAuth.ts), else the live meeting.status
 *  frames land on a different user's channel and the client's list never advances past its last snapshot.
 *  Resolution mirrors proxyAuth.ts: a login cookie, or an explicitly enabled local self-host key. */
function resolveUpstreamKey(req) {
  return resolveTerminalProxyKey(
    readCookieValue(req.headers.cookie, AUTH_COOKIE),
    process.env,
  );
}

process.on("unhandledRejection", (reason) => {
  logError("unhandled promise rejection", reason);
});

process.on("uncaughtException", (err) => {
  logError("uncaught exception", err);
});

const app = next({ dev, hostname, port });
const handle = app.getRequestHandler();

await app.prepare();

const server = createServer((req, res) => {
  Promise.resolve(handle(req, res)).catch((err) => {
    logError("request handler failed", err);
    sendProxyError(res);
  });
});

// Browser-facing WS server — we do the upgrade ourselves (noServer) only for `/ws`.
const wss = new WebSocketServer({
  noServer: true,
  maxPayload: WS_PROXY_LIMITS.maxPayloadBytes,
});

server.on("upgrade", (req, socket, head) => {
  let pathname;
  try {
    pathname = new URL(req.url, `http://${req.headers.host}`).pathname;
  } catch {
    socket.destroy();
    return;
  }
  if (pathname !== "/ws") {
    // Let Next/HMR handle its own upgrades (e.g. `_next/webpack-hmr` in dev).
    return;
  }
  // Resolve the per-user key from THIS request's cookie before the upgrade completes (req.headers are
  // gone once we hand off to the WS client). A missing/malformed cookie is rejected locally unless the
  // operator explicitly enabled local self-host shared-key mode.
  const apiKey = resolveUpstreamKey(req);
  if (!apiKey) {
    endSocket(socket, 401, "Unauthorized");
    return;
  }
  let closeOnSocketError = () => endSocket(socket);
  attachSocketError(socket, "client upgrade", () => closeOnSocketError());
  wss.handleUpgrade(req, socket, head, (client) => {
    closeOnSocketError = proxyToGateway(client, socket, apiKey);
  });
});

server.on("clientError", (err, socket) => {
  logError("http client socket error", err);
  endSocket(socket);
});

server.on("error", (err) => {
  logError("http server error", err);
});

wss.on("error", (err) => {
  logError("websocket server error", err);
});

function proxyToGateway(client, clientSocket, apiKey) {
  const target = `${GATEWAY_URL}/ws`;
  const upstream = new WebSocket(target, {
    headers: apiKey ? { "x-api-key": apiKey } : {},
    maxPayload: WS_PROXY_LIMITS.maxPayloadBytes,
  });

  const pending = new PendingFrameQueue({
    maxFrames: WS_PROXY_LIMITS.maxPendingFrames,
    maxBytes: WS_PROXY_LIMITS.maxPendingBytes,
  });
  let upstreamOpen = false;
  let cancelConnectTimeout = () => {};

  const closePair = () => {
    cancelConnectTimeout();
    pending.clear();
    safeClose(client);
    safeClose(upstream);
  };
  const onProxyError = (scope, err) => {
    logError(scope, err);
    closePair();
  };
  cancelConnectTimeout = armConnectTimeout(
    () => onProxyError("upstream websocket connect timeout", new Error("gateway websocket did not open in time")),
    WS_PROXY_LIMITS.connectTimeoutMs,
  );

  attachSocketError(clientSocket || client._socket, "client websocket", (err) => onProxyError("client websocket socket error", err));
  attachSocketError(upstream._socket, "upstream websocket", (err) => onProxyError("upstream websocket socket error", err));

  client.on("message", (data, isBinary) => {
    if (upstreamOpen && upstream.readyState === WebSocket.OPEN) {
      sendFrame(upstream, data, { binary: isBinary }, "client -> upstream", closePair);
    } else if (upstream.readyState === WebSocket.CONNECTING) {
      if (!pending.push(data, isBinary)) {
        onProxyError("client -> upstream queue overflow", new Error("websocket pending budget exceeded"));
      }
    }
  });

  upstream.on("open", () => {
    upstreamOpen = true;
    cancelConnectTimeout();
    attachSocketError(upstream._socket, "upstream websocket", (err) => onProxyError("upstream websocket socket error", err));
    for (const [data, isBinary] of pending.drain()) {
      sendFrame(upstream, data, { binary: isBinary }, "client -> upstream", closePair);
    }
  });
  upstream.on("upgrade", () => {
    attachSocketError(upstream._socket, "upstream websocket", (err) => onProxyError("upstream websocket socket error", err));
  });
  upstream.on("message", (data, isBinary) => {
    sendFrame(client, data, { binary: isBinary }, "upstream -> client", closePair);
  });
  upstream.on("unexpected-response", (_req, res) => {
    logError("upstream websocket rejected upgrade", new Error(`HTTP ${res.statusCode}`));
    closePair();
  });

  // Close each side when the other closes. Only forward a code if it's a valid
  // application close code (1000 / 3000-4999); reserved codes like 1005/1006
  // would throw, so fall back to a bare close.
  client.on("close", (code, reason) => {
    cancelConnectTimeout();
    pending.clear();
    safeClose(upstream, code, reason);
  });
  upstream.on("close", (code, reason) => {
    cancelConnectTimeout();
    pending.clear();
    safeClose(client, code, reason);
  });
  client.on("error", (err) => onProxyError("client websocket error", err));
  upstream.on("error", (err) => onProxyError("upstream websocket error", err));

  return closePair;
}

const socketErrorHandlers = new WeakSet();

function attachSocketError(socket, scope, onError) {
  if (!socket || socketErrorHandlers.has(socket)) return;
  socketErrorHandlers.add(socket);
  socket.on("error", (err) => {
    logError(scope, err);
    onError?.(err);
  });
}

function sendFrame(sock, data, options, scope, onError) {
  if (sock.readyState !== WebSocket.OPEN) return;
  if (!socketCanAcceptFrame(sock, data, WS_PROXY_LIMITS.maxBufferedBytes)) {
    const err = new Error("websocket backpressure budget exceeded");
    logError(`${scope} send refused`, err);
    onError?.(err);
    return;
  }
  try {
    sock.send(data, options, (err) => {
      if (!err) return;
      logError(`${scope} send failed`, err);
      onError?.(err);
    });
  } catch (err) {
    logError(`${scope} send failed`, err);
    onError?.(err);
  }
}

function safeClose(sock, code, reason) {
  if (!sock || sock.readyState === WebSocket.CLOSING || sock.readyState === WebSocket.CLOSED) return;
  try {
    if (code === 1000 || (code >= 3000 && code <= 4999)) sock.close(code, reason);
    else sock.close();
  } catch (err) {
    logError("websocket close failed", err);
  }
}

function sendProxyError(res) {
  if (res.destroyed || res.writableEnded) return;
  try {
    if (res.headersSent) {
      res.end();
      return;
    }
    res.writeHead(502, {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    });
    res.end(JSON.stringify({ error: "upstream_unavailable" }));
  } catch (err) {
    logError("failed to send proxy error response", err);
  }
}

function endSocket(socket, status = 400, reason = "Bad Request") {
  if (!socket || socket.destroyed) return;
  try {
    socket.end(`HTTP/1.1 ${status} ${reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  } catch (err) {
    logError("socket end failed", err);
  }
}

function logError(scope, err) {
  // eslint-disable-next-line no-console
  console.error(`[terminal-server] ${scope}`, err);
}

server.listen(port, hostname, () => {
  // eslint-disable-next-line no-console
  console.log(`> Terminal ready on http://${hostname}:${port} (WS proxy /ws -> ${GATEWAY_URL}/ws)`);
});
