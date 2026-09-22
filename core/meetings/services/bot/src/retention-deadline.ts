/** Absolute capture-deadline guard for ZAKI-managed Minutes workloads. */

export const RETENTION_FENCE_LEAD_MS = 5_000;
export const RETENTION_FENCE_BUDGET_MS = 4_000;
/** Stay comfortably below Node's signed-32-bit setTimeout ceiling; long deadlines re-arm. */
export const MAX_DEADLINE_TIMER_MS = 24 * 60 * 60 * 1_000;

type TimerHandle = ReturnType<typeof setTimeout> | unknown;

export function deadlineGuardDelayMs(expiresAt: string, nowMs = Date.now()): number {
  const expiresMs = Date.parse(expiresAt);
  if (!Number.isFinite(expiresMs)) throw new Error('capture retention deadline is invalid');
  return Math.max(0, expiresMs - nowMs - RETENTION_FENCE_LEAD_MS);
}

export async function enforceCaptureDeadline(opts: {
  /** Synchronously close every in-process content sink before any remote operation can block. */
  revoke: () => void;
  fence: () => Promise<void>;
  stop: () => void;
  onFenceFailure?: (error: Error) => void;
  fenceBudgetMs?: number;
}): Promise<void> {
  const budgetMs = opts.fenceBudgetMs ?? RETENTION_FENCE_BUDGET_MS;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    opts.revoke();
  } catch (error) {
    // A broken local adapter must not suppress the permanent fence or authoritative stop.
    opts.onFenceFailure?.(
      error instanceof Error ? error : new Error('capture sink revocation failed'),
    );
  }
  try {
    await Promise.race([
      opts.fence(),
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(
          () => reject(new Error('capture retention fence timed out')),
          budgetMs,
        );
      }),
    ]);
  } catch (error) {
    opts.onFenceFailure?.(
      error instanceof Error ? error : new Error('capture retention fence failed'),
    );
  } finally {
    if (timeout) clearTimeout(timeout);
    opts.stop();
  }
}

export function armCaptureDeadline(opts: {
  expiresAt: string;
  revoke: () => void;
  fence: () => Promise<void>;
  stop: () => void;
  onFenceFailure?: (error: Error) => void;
  nowMs?: () => number;
  schedule?: (callback: () => void, delayMs: number) => TimerHandle;
  cancel?: (handle: TimerHandle) => void;
}): () => void {
  const nowMs = opts.nowMs ?? Date.now;
  const schedule = opts.schedule ?? ((callback, delayMs) => setTimeout(callback, delayMs));
  const cancel = opts.cancel ?? ((handle) => clearTimeout(handle as ReturnType<typeof setTimeout>));
  let released = false;
  let handle: TimerHandle | undefined;
  const armNextChunk = (): void => {
    const remainingMs = deadlineGuardDelayMs(opts.expiresAt, nowMs());
    const delayMs = Math.min(remainingMs, MAX_DEADLINE_TIMER_MS);
    handle = schedule(() => {
      if (released) return;
      if (deadlineGuardDelayMs(opts.expiresAt, nowMs()) > 0) {
        armNextChunk();
        return;
      }
      void enforceCaptureDeadline({
        revoke: opts.revoke,
        fence: opts.fence,
        stop: () => { if (!released) opts.stop(); },
        onFenceFailure: opts.onFenceFailure,
      });
    }, delayMs);
  };
  armNextChunk();
  return () => {
    released = true;
    if (handle !== undefined) cancel(handle);
  };
}
