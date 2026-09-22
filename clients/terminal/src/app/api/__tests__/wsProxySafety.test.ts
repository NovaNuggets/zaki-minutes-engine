import { afterEach, describe, expect, it, vi } from "vitest";

import {
  PendingFrameQueue,
  WS_PROXY_LIMITS,
  armConnectTimeout,
  socketCanAcceptFrame,
} from "../../../wsProxySafety.mjs";

afterEach(() => vi.useRealTimers());


describe("Terminal WebSocket proxy safety", () => {
  it("rejects a pre-connect frame that would exceed the pending byte budget", () => {
    const pending = new PendingFrameQueue({ maxFrames: 4, maxBytes: 8 });

    expect(pending.push(Buffer.from("12345"), false)).toBe(true);
    expect(pending.push(Buffer.from("6789"), false)).toBe(false);
    expect(pending.bytes).toBe(5);
    expect(pending.length).toBe(1);
  });

  it("refuses a write when socket backlog plus the frame crosses the high-water mark", () => {
    expect(socketCanAcceptFrame({ bufferedAmount: 6 }, Buffer.from("12"), 8)).toBe(true);
    expect(socketCanAcceptFrame({ bufferedAmount: 7 }, Buffer.from("12"), 8)).toBe(false);
  });

  it("arms a finite upstream-connect deadline that can be cancelled after open", () => {
    vi.useFakeTimers();
    const expired = vi.fn();
    const cancel = armConnectTimeout(expired, 25);

    vi.advanceTimersByTime(24);
    expect(expired).not.toHaveBeenCalled();
    cancel();
    vi.advanceTimersByTime(1);
    expect(expired).not.toHaveBeenCalled();
    expect(WS_PROXY_LIMITS.connectTimeoutMs).toBeGreaterThan(0);
    expect(WS_PROXY_LIMITS.maxPayloadBytes).toBeLessThanOrEqual(1024 * 1024);
  });
});
