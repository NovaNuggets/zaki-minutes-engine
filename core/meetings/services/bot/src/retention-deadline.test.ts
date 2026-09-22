/** Active absolute-retention deadline: fence both Redis scopes before stopping capture. */
import {
  MAX_DEADLINE_TIMER_MS,
  RETENTION_FENCE_LEAD_MS,
  armCaptureDeadline,
  deadlineGuardDelayMs,
  enforceCaptureDeadline,
} from './retention-deadline.js';
import {
  createRedisTranscriptSink,
  type RedisTranscriptClient,
} from './adapters/transcript-redis.js';
import { createBotRecordingSink } from './recording.js';
import type { TranscriptSegment } from './contracts.js';

let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond ? '' : ` — ${detail}`}`);
  if (!cond) failed++;
};

async function main(): Promise<void> {
  const now = Date.parse('2026-07-15T08:00:00Z');
  const deadline = '2026-07-15T08:01:00Z';
  check(
    'guard starts before the absolute deadline',
    deadlineGuardDelayMs(deadline, now) === 60_000 - RETENTION_FENCE_LEAD_MS,
    String(deadlineGuardDelayMs(deadline, now)),
  );
  check(
    'already-due guard fires immediately',
    deadlineGuardDelayMs(deadline, now + 60_000) === 0,
  );

  {
    const events: string[] = [];
    await enforceCaptureDeadline({
      revoke: () => { events.push('revoke'); },
      fence: async () => { events.push('fence'); },
      stop: () => { events.push('stop'); },
    });
    check(
      'successful deadline latches local sinks, then fences, then stops',
      JSON.stringify(events) === JSON.stringify(['revoke', 'fence', 'stop']),
      JSON.stringify(events),
    );
  }

  {
    const events: string[] = [];
    await enforceCaptureDeadline({
      revoke: () => { events.push('revoke'); },
      fence: () => new Promise<void>(() => {}),
      stop: () => { events.push('stop'); },
      onFenceFailure: (error) => { events.push(`error:${error.message}`); },
      fenceBudgetMs: 1,
    });
    check(
      'hung fence is bounded and cannot suppress the prior latch or stop',
      events[0] === 'revoke'
        && events[1]?.startsWith('error:capture retention fence timed out') === true
        && events[2] === 'stop',
      JSON.stringify(events),
    );
  }

  {
    const events: string[] = [];
    await enforceCaptureDeadline({
      revoke: () => { events.push('revoke'); },
      fence: async () => { events.push('fence'); throw new Error('private redis detail'); },
      stop: () => { events.push('stop'); },
      onFenceFailure: (error) => { events.push(`error:${error.name}`); },
    });
    check(
      'fence failure is reported without skipping stop',
      JSON.stringify(events) === JSON.stringify(['revoke', 'fence', 'error:Error', 'stop']),
      JSON.stringify(events),
    );
  }

  // A Redis outage can outlast the remote-fence budget. The local latch is synchronous and must
  // still reject the pipeline's final transcript/recording flush after Redis recovers.
  {
    let redisWrites = 0;
    const client: RedisTranscriptClient = {
      async xAdd() { return '1-0'; },
      async publish() { return 1; },
      async writeTranscriptIfWritable() { redisWrites++; return true; },
    };
    const transcript = createRedisTranscriptSink({ client, meetingId: 42 });
    let recordingUploads = 0;
    const recording = createBotRecordingSink({
      inv: {
        platform: 'google_meet', meetingUrl: 'https://meet.google.com/abc-defg-hij',
        botName: 'ZAKI Notetaker', redisUrl: 'redis://redis:6379', recordingEnabled: true,
      },
      onMaster: () => { recordingUploads++; },
    });
    const segment: TranscriptSegment = {
      segment_id: 'session:final', speaker: 'Alice', speaker_key: 's1', text: 'final words',
      start: 0, end: 1, completed: true, source: 'glow-bound',
    };
    recording.chunk('google_meet/42', 0, false, 'webm', Buffer.from([1, 2, 3]));
    let finalTranscriptFlush: Promise<void> | undefined;
    let transcriptRejected = false;

    await enforceCaptureDeadline({
      revoke: () => { transcript.revoke(); recording.revoke(); },
      fence: () => new Promise<void>(() => {}),
      fenceBudgetMs: 1,
      stop: () => {
        // Simulate Redis becoming writable precisely when graceful pipeline.stop() flushes.
        finalTranscriptFlush = transcript.publish(segment).catch(() => { transcriptRejected = true; });
        recording.close('google_meet/42');
      },
    });
    await finalTranscriptFlush;

    check('fence timeout + Redis recovery: final transcript flush is locally rejected', transcriptRejected);
    check('fence timeout + Redis recovery: no Redis transcript append occurs', redisWrites === 0, String(redisWrites));
    check('fence timeout + final recording flush: no upload is assembled', recordingUploads === 0, String(recordingUploads));
  }

  {
    let scheduledDelay = -1;
    let cancelled = false;
    const release = armCaptureDeadline({
      expiresAt: deadline,
      nowMs: () => now,
      revoke: () => {},
      fence: async () => {},
      stop: () => {},
      schedule: (_callback, delayMs) => { scheduledDelay = delayMs; return 17; },
      cancel: (handle) => { cancelled = handle === 17; },
    });
    release();
    check(
      'arm uses the bounded pre-deadline lead',
      scheduledDelay === 60_000 - RETENTION_FENCE_LEAD_MS,
      String(scheduledDelay),
    );
    check('release cancels the deadline timer', cancelled);
  }

  // Node clamps a setTimeout delay above 2^31-1ms to roughly 1ms. Long retention windows must
  // therefore re-arm bounded chunks instead of terminating a meeting immediately.
  {
    type Scheduled = { callback: () => void; delayMs: number; handle: number };
    const scheduled: Scheduled[] = [];
    const cancelled: number[] = [];
    const longStart = Date.parse('2026-07-15T08:00:00Z');
    const longDeadlineMs = longStart + 30 * 24 * 60 * 60 * 1000;
    let clock = longStart;
    let stopped = 0;
    const release = armCaptureDeadline({
      expiresAt: new Date(longDeadlineMs).toISOString(),
      nowMs: () => clock,
      revoke: () => {},
      fence: async () => {},
      stop: () => { stopped++; },
      schedule: (callback, delayMs) => {
        const handle = scheduled.length + 1;
        scheduled.push({ callback, delayMs, handle });
        return handle;
      },
      cancel: (handle) => { cancelled.push(handle as number); },
    });

    check(
      '30-day deadline: first timer chunk stays below the Node overflow boundary',
      scheduled[0]?.delayMs === MAX_DEADLINE_TIMER_MS
        && scheduled[0].delayMs < 2_147_483_647,
      String(scheduled[0]?.delayMs),
    );
    check('30-day deadline: no immediate stop', stopped === 0, String(stopped));

    clock = longStart + MAX_DEADLINE_TIMER_MS;
    scheduled.shift()?.callback();
    check(
      '30-day deadline: chunk callback re-arms against the injected clock',
      scheduled[0]?.delayMs === MAX_DEADLINE_TIMER_MS,
      String(scheduled[0]?.delayMs),
    );
    clock = longDeadlineMs - RETENTION_FENCE_LEAD_MS;
    scheduled.shift()?.callback();
    await Promise.resolve();
    await Promise.resolve();
    check('30-day deadline: stop occurs only at the guard instant', stopped === 1, String(stopped));
    release();
    check('30-day deadline: release cancels the current handle', cancelled.length === 1, JSON.stringify(cancelled));
  }

  if (failed) {
    console.error(`\n❌ retention deadline: ${failed} check(s) FAILED.`);
    process.exit(1);
  }
  console.log('\n✅ retention deadline: raw+processed fence precedes a bounded, authoritative capture stop.');
}

void main();
