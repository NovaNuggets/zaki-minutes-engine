/**
 * Version-routed invocation boot config (P14) — the bot's "constructor".
 *
 * The container is started with ONE JSON env var, `VEXA_BOT_CONFIG`, holding an
 * `invocation.v1` object. We validate it at boot against the PUBLISHED schema
 * (`meetings/contracts/invocation.v1/invocation.schema.json`, loaded by PATH — the
 * goldens are the spec, P8) with ajv — the same validator the contract's own
 * `validate.mjs` uses, so the bot can NEVER drift from the contract. A parse/validation
 * failure is fatal: the caller maps it to a lifecycle.v1 `failed` / `validation_error`
 * (fail-fast, P14). Scoped secrets ride in this contract (MeetingToken / STT / S3 keys) —
 * never logged (P14/P15).
 *
 * `Invocation` is the typed view the rest of the bot depends on. It is a hand-written
 * mirror of the schema's `#/$defs/Invocation` (no zod — zero new runtime deps; ajv is the
 * single source of truth at runtime, this interface is the compile-time shadow).
 */
import { Ajv2020 } from 'ajv/dist/2020.js';
import type { Ajv, ValidateFunction } from 'ajv';
import addFormatsDefault from 'ajv-formats';
import { readFileSync } from 'node:fs';

// verbatimModuleSyntax (tsconfig.base): the CJS default export of ajv-formats isn't
// synthesized as callable, so bind its call signature explicitly. Runtime is unchanged.
const addFormats = addFormatsDefault as unknown as (ajv: Ajv) => Ajv;
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

export type Platform = 'google_meet' | 'zoom' | 'teams' | 'jitsi';
export type TranscriptionTier = 'realtime' | 'deferred';
export type InvocationContract = 'invocation.v1' | 'invocation.v2';

/** True for platforms that ride the MIXED capture lane (one combined WebRTC audio
 *  stream + pyannote separation); google_meet rides the per-channel gmeet lane.
 *  The ONE predicate the browser hook, the capture bridge, and the pipeline pick
 *  must all agree on — never restate it inline. */
export function isMixedLanePlatform(p: Platform | string): boolean {
  return p === 'zoom' || p === 'teams' || p === 'jitsi';
}

export interface AutomaticLeave {
  waitingRoomTimeout?: number;
  noOneJoinedTimeout?: number;
  everyoneLeftTimeout?: number;
}

export interface ManagedRetention {
  policyVersion: string;
  scopeExpiresAt: { audio: string; transcript: string; summary: string };
}

/** The compile-time mirror of invocation.v1 `#/$defs/Invocation` (ajv is the runtime truth). */
export interface Invocation {
  /** Present and fixed only on the opt-in managed v2 wire. */
  contractVersion?: 'invocation.v2';
  // ── what to join (v1 requires redisUrl; managed v2 requires the mediated HTTP URLs) ──
  platform: Platform;
  meetingUrl: string | null;
  botName: string;
  passcode?: string;
  nativeMeetingId?: string;
  // ── identity / control plane ──
  token?: string;
  connectionId?: string;
  meeting_id?: number | string;
  container_name?: string;
  redisUrl?: string;
  transcriptIngestUrl?: string;
  retentionFenceUrl?: string;
  meetingApiCallbackUrl?: string;
  /** Legacy schema-compatible field accepted only when reading an older invocation; unused. */
  internalSecret?: string;
  // ── transcription ──
  language?: string | null;
  task?: string | null;
  allowedLanguages?: string[];
  transcribeEnabled?: boolean;
  transcriptionTier?: TranscriptionTier;
  transcriptionServiceUrl?: string;
  transcriptionServiceToken?: string;
  // ── recording ──
  recordingEnabled?: boolean;
  captureModes?: string[];
  recordingUploadUrl?: string;
  /** Absolute earliest audio/transcript/summary deadline for managed capture. */
  captureExpiresAt?: string;
  managedRetention?: ManagedRetention;
  // ── lifecycle timeouts ──
  automaticLeave?: AutomaticLeave;
  reconnectionIntervalMs?: number;
  // ── voice agent (gates acts.v1 voice commands; DEFERRED in this increment) ──
  voiceAgentEnabled?: boolean;
  defaultAvatarUrl?: string;
  videoReceiveEnabled?: boolean;
  cameraEnabled?: boolean;
  // ── authenticated meeting bot (persistent browser context from S3) ──
  authenticated?: boolean;
  userdataS3Path?: string;
  s3Endpoint?: string;
  s3Bucket?: string;
  s3AccessKey?: string;
  s3SecretKey?: string;
}

/** Thrown when VEXA_BOT_CONFIG is missing / not JSON / off-contract. The composition root
 *  maps this to lifecycle.v1 failed(validation_error, failure_stage=requested). */
export class InvocationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvocationError';
  }
}

const HERE = dirname(fileURLToPath(import.meta.url));
const schemaPath = (version: InvocationContract) =>
  join(HERE, '..', '..', '..', 'contracts', version, 'invocation.schema.json');

interface Validator { ajv: Ajv; validate: ValidateFunction }
const _validators = new Map<InvocationContract, Validator>();
function validator(version: InvocationContract): Validator {
  const existing = _validators.get(version);
  if (existing) return existing;
  const schema = JSON.parse(readFileSync(schemaPath(version), 'utf8'));
  const ajv = new Ajv2020({ strict: false, allErrors: true });
  addFormats(ajv);
  ajv.addSchema(schema);
  const validate = ajv.compile({ $ref: `${schema.$id}#/$defs/Invocation` });
  const built = { ajv, validate };
  _validators.set(version, built);
  return built;
}

/** Parse + validate a raw JSON string against the explicitly selected contract. */
export function parseInvocation(
  raw: string | undefined,
  version: InvocationContract = 'invocation.v1',
): Invocation {
  if (!raw || !raw.trim()) throw new InvocationError(`${version}: VEXA_BOT_CONFIG env is missing or empty`);
  let data: unknown;
  try {
    data = JSON.parse(raw);
  } catch (e) {
    throw new InvocationError(`${version}: VEXA_BOT_CONFIG is not valid JSON — ${(e as Error).message}`);
  }
  const { ajv, validate } = validator(version);
  if (!validate(data)) {
    throw new InvocationError(`${version}: VEXA_BOT_CONFIG failed validation — ${ajv.errorsText(validate.errors)}`);
  }
  if (version === 'invocation.v2') {
    const invocation = data as Invocation;
    const expiries = invocation.managedRetention?.scopeExpiresAt;
    const scopeMs = expiries
      ? [expiries.audio, expiries.transcript, expiries.summary].map((value) => Date.parse(value))
      : [];
    const captureMs = Date.parse(invocation.captureExpiresAt ?? '');
    if (scopeMs.length !== 3
        || scopeMs.some((value) => !Number.isFinite(value))
        || !Number.isFinite(captureMs)
        || captureMs !== Math.min(...scopeMs)) {
      throw new InvocationError(
        'invocation.v2: captureExpiresAt must equal the earliest managed retention expiry',
      );
    }
  }
  return data as Invocation;
}

/** Boot helper — read VEXA_BOT_CONFIG from the environment and validate it (P7: config by env). */
export function loadInvocation(env: NodeJS.ProcessEnv = process.env): Invocation {
  const selected = env.VEXA_INVOCATION_CONTRACT?.trim() || 'invocation.v1';
  if (selected !== 'invocation.v1' && selected !== 'invocation.v2') {
    throw new InvocationError(`unsupported invocation contract selector: ${selected}`);
  }
  return parseInvocation(env.VEXA_BOT_CONFIG, selected);
}
