/**
 * L1/L2 — version-routed invocation boot config. Drives the real ajv-backed parsers against
 * the published v1 and v2 goldens and asserts:
 *   • every committed positive golden (minimal + full + jitsi) parses;
 *   • the env helper round-trips VEXA_BOT_CONFIG;
 *   • off-contract input (missing required, unknown action, bad enum, non-JSON, absent)
 *     fails fast with an InvocationError (P14).
 * No browser / redis / STT. Run: npx tsx src/config.test.ts
 */
import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseInvocation, loadInvocation, InvocationError } from './config.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const V1_GOLDEN_DIR = join(HERE, '..', '..', '..', 'contracts', 'invocation.v1', 'golden');
const V2_GOLDEN_DIR = join(HERE, '..', '..', '..', 'contracts', 'invocation.v2', 'golden');

let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond ? '' : '  — ' + detail}`);
  if (!cond) failed++;
};
const throws = (fn: () => unknown): Error | null => { try { fn(); return null; } catch (e) { return e as Error; } };

// ── every committed positive invocation.v1 golden parses ──
// Negative controls deliberately fail this parser and are exercised below and by validate.mjs.
const goldens = readdirSync(V1_GOLDEN_DIR).filter((n) =>
  n.startsWith('Invocation.') && n.endsWith('.json') && !n.includes('.invalid-'));
check('found all invocation goldens', goldens.length === 3, `got ${goldens.join(', ')}`);
for (const g of goldens) {
  const raw = readFileSync(join(V1_GOLDEN_DIR, g), 'utf8');
  const err = throws(() => parseInvocation(raw));
  check(`golden ${g} parses`, err === null, err?.message ?? '');
}

// ── typed access on the full golden ──
{
  const full = parseInvocation(readFileSync(join(V1_GOLDEN_DIR, 'Invocation.full.json'), 'utf8'));
  check('full: platform = google_meet', full.platform === 'google_meet', full.platform);
  check('full: recordingEnabled true', full.recordingEnabled === true);
  check('full: automaticLeave threaded', full.automaticLeave?.waitingRoomTimeout === 300000, String(full.automaticLeave?.waitingRoomTimeout));
  check('full: managed deadline absent on v1', full.captureExpiresAt === undefined, String(full.captureExpiresAt));
  check('full: secret token present (not logged)', typeof full.token === 'string' && full.token.length > 0);
}

// ── typed access on the jitsi golden (the platform enum accepts jitsi) ──
{
  const jitsi = parseInvocation(readFileSync(join(V1_GOLDEN_DIR, 'Invocation.jitsi.json'), 'utf8'));
  check('jitsi: platform = jitsi', jitsi.platform === 'jitsi', jitsi.platform);
  check('jitsi: meetingUrl carries the deployment host', jitsi.meetingUrl === 'https://meet.jit.si/VexaStandup', String(jitsi.meetingUrl));
}

// ── the env helper (P7: config by env) ──
{
  const minimal = readFileSync(join(V1_GOLDEN_DIR, 'Invocation.minimal.json'), 'utf8');
  const inv = loadInvocation({ VEXA_BOT_CONFIG: minimal } as NodeJS.ProcessEnv);
  check('loadInvocation reads VEXA_BOT_CONFIG', inv.botName === 'Vexa', inv.botName);
}

// ── v2 is opt-in and never reaches the v1 parser ──
{
  const managed = readFileSync(join(V2_GOLDEN_DIR, 'Invocation.managed.json'), 'utf8');
  check('old v1 consumer rejects a v2 producer', throws(() => parseInvocation(managed)) instanceof InvocationError);
  const parsed = parseInvocation(managed, 'invocation.v2');
  check('v2 parser accepts managed golden', parsed.contractVersion === 'invocation.v2');
  check('v2 meeting id stays a canonical decimal string', parsed.meeting_id === '9223372036854775807', String(parsed.meeting_id));
  check('v2 managed deadline is available', parsed.captureExpiresAt === '2026-07-17T12:00:00Z', String(parsed.captureExpiresAt));
  check('v2 carries no Redis authority', parsed.redisUrl === undefined, String(parsed.redisUrl));
  check('v2 uses mediated transcript ingress', parsed.transcriptIngestUrl?.endsWith('/bots/internal/transcripts/ingest') === true, String(parsed.transcriptIngestUrl));
  const managedValue = JSON.parse(managed) as Record<string, unknown>;
  for (const required of ['token', 'connectionId', 'meetingApiCallbackUrl', 'transcriptionServiceUrl', 'recordingUploadUrl'] as const) {
    const incomplete = { ...managedValue, [required]: undefined };
    delete incomplete[required];
    check(`v2 rejects missing managed ${required}`, throws(() => parseInvocation(JSON.stringify(incomplete), 'invocation.v2')) instanceof InvocationError);
  }
  const loaded = loadInvocation({
    VEXA_BOT_CONFIG: managed,
    VEXA_INVOCATION_CONTRACT: 'invocation.v2',
  } as NodeJS.ProcessEnv);
  check('explicit env selector routes v2', loaded.contractVersion === 'invocation.v2');
  check('missing selector cannot route v2', throws(() => loadInvocation({ VEXA_BOT_CONFIG: managed } as NodeJS.ProcessEnv)) instanceof InvocationError);
  check('unknown selector fails closed', throws(() => loadInvocation({
    VEXA_BOT_CONFIG: managed,
    VEXA_INVOCATION_CONTRACT: 'invocation.v99',
  } as NodeJS.ProcessEnv)) instanceof InvocationError);
}

// ── fail-fast (P14) ──
{
  check('missing env → InvocationError', throws(() => loadInvocation({} as NodeJS.ProcessEnv)) instanceof InvocationError);
  check('empty string → InvocationError', throws(() => parseInvocation('   ')) instanceof InvocationError);
  check('non-JSON → InvocationError', throws(() => parseInvocation('not json {')) instanceof InvocationError);
  check('missing required field → InvocationError',
    throws(() => parseInvocation(JSON.stringify({ platform: 'google_meet', botName: 'B' }))) instanceof InvocationError);
  check('unknown property (additionalProperties:false) → InvocationError',
    throws(() => parseInvocation(JSON.stringify({ platform: 'google_meet', meetingUrl: 'x', botName: 'B', redisUrl: 'redis://r', bogus: 1 }))) instanceof InvocationError);
  check('bad platform enum → InvocationError',
    throws(() => parseInvocation(JSON.stringify({ platform: 'webex', meetingUrl: 'x', botName: 'B', redisUrl: 'redis://r' }))) instanceof InvocationError);
  check('managed v2 property on v1 → InvocationError',
    throws(() => parseInvocation(JSON.stringify({
      platform: 'google_meet', meetingUrl: 'x', botName: 'B', redisUrl: 'redis://r',
      captureExpiresAt: '2026-07-15T09:00:00Z',
    }))) instanceof InvocationError);
}

if (failed) { console.error(`\n❌ config (L1/L2): ${failed} check(s) FAILED.`); process.exit(1); }
console.log('\n✅ config (L1/L2): v1 stays compatible, v2 is explicit, and mixed rollout fails closed.');
