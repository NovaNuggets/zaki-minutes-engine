/** Bounded STT response gate: a hosted backend is untrusted input. Success bodies must be
 * byte-bounded before JSON parsing, fanout-bounded before mapping, and malformed payloads must
 * become local typed faults without reflecting backend content. */
import {
  MAX_TRANSCRIPTION_AUDIO_SECONDS,
  MAX_TRANSCRIPTION_PROMPT_CHARS,
  MAX_TRANSCRIPTION_RESPONSE_BYTES,
  TranscriptionClient,
  TranscriptionError,
  validTranscriptionResponse,
} from './index.js';

let failed = 0;
const check = (name: string, condition: boolean, detail = '') => {
  console.log(`  ${condition ? '✅' : '❌'} ${name}${condition ? '' : `  — ${detail}`}`);
  if (!condition) failed++;
};

const realFetch = globalThis.fetch;
const pcm = new Float32Array(1600).fill(0.05);
const valid = {
  text: 'hello',
  language: 'en',
  duration: 0.1,
  segments: [],
};

async function faultOf(client: TranscriptionClient): Promise<TranscriptionError | null> {
  try {
    await client.transcribe(pcm, 'en');
    return null;
  } catch (error) {
    return error instanceof TranscriptionError ? error : null;
  }
}

async function run() {
  {
    (globalThis as any).fetch = async () => new Response(JSON.stringify(valid), {
      status: 200,
      headers: { 'Content-Length': String(MAX_TRANSCRIPTION_RESPONSE_BYTES + 1) },
    });
    const client = new TranscriptionClient({ serviceUrl: 'http://stt.test', maxRetries: 0 });
    const fault = await faultOf(client);
    check('declared oversized success is rejected before JSON parsing',
      fault?.source === 'stt' && fault.detail === 'invalid response');
  }

  {
    let pulls = 0;
    let cancelled = false;
    (globalThis as any).fetch = async () => new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        pulls++;
        if (pulls <= 10) controller.enqueue(new Uint8Array(1024 * 1024).fill(0x20));
        else controller.close();
      },
      cancel() { cancelled = true; },
    }), { status: 200 });
    const client = new TranscriptionClient({ serviceUrl: 'http://stt.test', maxRetries: 0 });
    const fault = await faultOf(client);
    check('chunked oversized success is rejected as a typed local fault',
      fault?.source === 'stt' && fault.detail === 'invalid response');
    check('chunked response reader cancels on the first crossing chunk',
      cancelled && pulls < 11, `cancelled=${cancelled}, pulls=${pulls}`);
  }

  {
    (globalThis as any).fetch = async () => new Response(JSON.stringify({
      ...valid,
      segments: Array.from({ length: 20_001 }, () => ({ start: 0, end: 1, text: 'x' })),
    }), { status: 200 });
    const client = new TranscriptionClient({ serviceUrl: 'http://stt.test', maxRetries: 0 });
    const fault = await faultOf(client);
    check('attacker-controlled segment fanout is rejected before mapping',
      fault?.source === 'stt' && fault.detail === 'invalid response');
  }

  {
    (globalThis as any).fetch = async () => new Response(JSON.stringify({
      ...valid,
      segments: [{ start: 0, end: 1, text: 'x', words: [
        { word: 'hello', start: 'not-a-number', end: 1, probability: 0.9 },
      ] }],
    }), { status: 200 });
    const client = new TranscriptionClient({ serviceUrl: 'http://stt.test', maxRetries: 0 });
    const fault = await faultOf(client);
    check('malformed nested word fields are rejected before use',
      fault?.source === 'stt' && fault.detail === 'invalid response');
  }

  {
    check('negative and reversed segment timestamps are rejected',
      !validTranscriptionResponse({
        ...valid,
        segments: [{ start: -1, end: 1, text: 'x' }],
      }, 2)
      && !validTranscriptionResponse({
        ...valid,
        segments: [{ start: 2, end: 1, text: 'x' }],
      }, 2));
    check('negative, reversed, and out-of-window word timestamps are rejected',
      !validTranscriptionResponse({
        ...valid,
        segments: [{ start: 0, end: 2, text: 'x', words: [
          { word: 'x', start: -1, end: 1, probability: 0.9 },
       ] }],
      }, 2)
      && !validTranscriptionResponse({
        ...valid,
        segments: [{ start: 0, end: 2, text: 'x', words: [
          { word: 'x', start: 1.5, end: 1, probability: 0.9 },
       ] }],
      }, 2)
      && !validTranscriptionResponse({
        ...valid,
        segments: [{ start: 0, end: 2, text: 'x', words: [
          { word: 'x', start: 1, end: 1e200, probability: 0.9 },
       ] }],
      }, 2));

    (globalThis as any).fetch = async () => new Response(JSON.stringify({
      ...valid,
      duration: 1e200,
      segments: [{ start: 0, end: 1e200, text: 'hostile offset' }],
    }), { status: 200 });
    const client = new TranscriptionClient({ serviceUrl: 'http://stt.test', maxRetries: 0 });
    const fault = await faultOf(client);
    check('the live client bounds timestamps to submitted audio plus tolerance',
      fault?.source === 'stt' && fault.detail === 'invalid response');
  }

  {
    let calls = 0;
    (globalThis as any).fetch = async () => {
      calls++;
      return new Response(JSON.stringify(valid), { status: 200 });
    };
    const client = new TranscriptionClient({
      serviceUrl: 'http://stt.test',
      sampleRate: 1,
      maxRetries: 0,
    });
    const oversizedAudio = new Float32Array(MAX_TRANSCRIPTION_AUDIO_SECONDS + 1);
    const fault = await faultOf({
      transcribe: () => client.transcribe(oversizedAudio, 'en'),
    } as TranscriptionClient);
    check('oversized outbound audio is rejected before WAV allocation or fetch',
      fault?.kind === 'bad_request' && calls === 0, `calls=${calls}, fault=${fault?.message}`);
  }

  {
    let sentBody = '';
    (globalThis as any).fetch = async (_input: unknown, init?: RequestInit) => {
      sentBody = Buffer.isBuffer(init?.body) ? init.body.toString() : '';
      return new Response(JSON.stringify(valid), { status: 200 });
    };
    const client = new TranscriptionClient({ serviceUrl: 'http://stt.test', maxRetries: 0 });
    await client.transcribe(pcm, 'en', 'x'.repeat(MAX_TRANSCRIPTION_PROMPT_CHARS + 10_000));
    check('outbound prompt is capped before multipart serialization',
      sentBody.includes('x'.repeat(MAX_TRANSCRIPTION_PROMPT_CHARS))
      && !sentBody.includes('x'.repeat(MAX_TRANSCRIPTION_PROMPT_CHARS + 1)));
  }

  {
    const originalNow = Date.now;
    Date.now = () => 1_700_000_000_000;
    const boundaries: string[] = [];
    (globalThis as any).fetch = async (_input: unknown, init?: RequestInit) => {
      const contentType = new Headers(init?.headers).get('content-type') ?? '';
      boundaries.push(contentType.split('boundary=')[1] ?? '');
      return new Response(JSON.stringify(valid), { status: 200 });
    };
    try {
      const client = new TranscriptionClient({ serviceUrl: 'http://stt.test', maxRetries: 0 });
      await client.transcribe(pcm, 'en', 'participant-controlled transcript');
      await client.transcribe(pcm, 'en', 'participant-controlled transcript');
    } finally {
      Date.now = originalNow;
    }
    check('multipart boundaries remain unguessable even within the same millisecond',
      boundaries.length === 2 && boundaries[0] !== boundaries[1], JSON.stringify(boundaries));
  }

  {
    const rejects = (config: Record<string, number>) => {
      try {
        new TranscriptionClient({ serviceUrl: 'http://stt.test', ...config });
        return false;
      } catch (error) {
        return error instanceof TypeError;
      }
    };
    check('retry and segmentation numeric settings reject unsafe values',
      rejects({ maxRetries: -1 })
      && rejects({ retryDelayMs: Number.NaN })
      && rejects({ maxSpeechDurationSec: -1 })
      && rejects({ minSilenceDurationMs: Number.POSITIVE_INFINITY }));
  }

  (globalThis as any).fetch = realFetch;
  if (failed) {
    console.error(`\n❌ stt bounded responses: ${failed} check(s) FAILED.`);
    process.exit(1);
  }
  console.log('\n✅ stt bounded responses: byte and fanout limits fail closed before parsing/mapping.');
}

run().catch((error) => {
  (globalThis as any).fetch = realFetch;
  console.error(error);
  process.exit(1);
});
