import { Buffer } from "node:buffer";


export const WS_PROXY_LIMITS = Object.freeze({
  maxPayloadBytes: 1024 * 1024,
  maxPendingFrames: 64,
  maxPendingBytes: 256 * 1024,
  maxBufferedBytes: 1024 * 1024,
  connectTimeoutMs: 5_000,
});


/** @param {() => void} onTimeout @param {number} [delayMs] */
export function armConnectTimeout(onTimeout, delayMs = WS_PROXY_LIMITS.connectTimeoutMs) {
  const timer = setTimeout(onTimeout, delayMs);
  timer.unref?.();
  return () => clearTimeout(timer);
}


export function frameByteLength(data) {
  if (typeof data === "string") return Buffer.byteLength(data);
  if (Array.isArray(data)) return data.reduce((total, part) => total + frameByteLength(part), 0);
  if (data && typeof data.byteLength === "number") return data.byteLength;
  if (data && typeof data.size === "number") return data.size;
  return Buffer.byteLength(String(data ?? ""));
}


export function socketCanAcceptFrame(socket, data, maxBufferedBytes) {
  const buffered = Number(socket?.bufferedAmount || 0);
  return buffered + frameByteLength(data) <= maxBufferedBytes;
}


export class PendingFrameQueue {
  #frames = [];
  #bytes = 0;

  constructor({ maxFrames, maxBytes }) {
    this.maxFrames = maxFrames;
    this.maxBytes = maxBytes;
  }

  get length() { return this.#frames.length; }
  get bytes() { return this.#bytes; }

  push(data, isBinary) {
    const bytes = frameByteLength(data);
    if (this.#frames.length + 1 > this.maxFrames || this.#bytes + bytes > this.maxBytes) {
      return false;
    }
    this.#frames.push([data, isBinary]);
    this.#bytes += bytes;
    return true;
  }

  drain() {
    const frames = this.#frames;
    this.#frames = [];
    this.#bytes = 0;
    return frames;
  }

  clear() {
    this.#frames = [];
    this.#bytes = 0;
  }
}
