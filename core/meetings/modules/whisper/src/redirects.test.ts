/** STT egress must never follow a redirect: meeting audio and credentials stay bound to the
 * operator-approved endpoint. Direct operator-owned in-cluster HTTP remains supported. */
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';

import { TranscriptionClient, TranscriptionError } from './index.js';

let failed = 0;
const check = (name: string, condition: boolean, detail = '') => {
  console.log(`  ${condition ? '✅' : '❌'} ${name}${condition ? '' : `  — ${detail}`}`);
  if (!condition) failed++;
};

async function serve(handler: (request: IncomingMessage, response: ServerResponse) => void) {
  const server = createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    baseUrl: `http://127.0.0.1:${port}`,
    close: () => new Promise<void>((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    }),
  };
}

const success = JSON.stringify({
  text: 'hello',
  language: 'en',
  duration: 0.1,
  segments: [],
});

async function run() {
  const pcm = new Float32Array(1600).fill(0.05);

  let redirectedRequests = 0;
  let redirectedBytes = 0;
  let redirectedAuthorization = '';
  const redirectedSink = await serve((request, response) => {
    redirectedRequests++;
    redirectedAuthorization = request.headers.authorization ?? '';
    request.on('data', (chunk: Buffer) => { redirectedBytes += chunk.length; });
    request.on('end', () => {
      response.writeHead(200, { 'Content-Type': 'application/json' });
      response.end(success);
    });
  });
  const redirecting = await serve((request, response) => {
    if (request.url === '/v1/audio/transcriptions') {
      // Consume the POST before replying so a client failure cannot be caused merely by
      // an early-response socket race. A default-follow client will replay the body.
      request.resume();
      request.on('end', () => {
        response.writeHead(307, { Location: `${redirectedSink.baseUrl}/redirected-sink` });
        response.end();
      });
      return;
    }
    request.resume();
    response.writeHead(404);
    response.end();
  });
  try {
    const client = new TranscriptionClient({
      serviceUrl: redirecting.baseUrl,
      apiToken: 'meeting-secret',
      maxRetries: 0,
    });
    const originalFetch = globalThis.fetch;
    let redirectMode: RequestRedirect | undefined;
    globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
      redirectMode = init?.redirect;
      return originalFetch(input, init);
    }) as typeof fetch;
    let fault: unknown;
    try {
      await client.transcribe(pcm, 'en');
    } catch (error) {
      fault = error;
    } finally {
      globalThis.fetch = originalFetch;
    }
    check('request sets an explicit no-follow redirect policy', redirectMode === 'error',
      `redirect=${JSON.stringify(redirectMode)}`);
    check('redirect response is rejected as an attributed STT fault', fault instanceof TranscriptionError);
    check('redirect target receives no meeting audio or credentials', redirectedRequests === 0,
      `redirectedRequests=${redirectedRequests}, bytes=${redirectedBytes}, auth=${JSON.stringify(redirectedAuthorization)}`);
  } finally {
    await redirecting.close();
    await redirectedSink.close();
  }

  let directRequests = 0;
  let directAuthorization = '';
  let directBytes = 0;
  let directPath = '';
  const direct = await serve((request, response) => {
    directRequests++;
    directPath = request.url ?? '';
    directAuthorization = request.headers.authorization ?? '';
    request.on('data', (chunk: Buffer) => { directBytes += chunk.length; });
    request.on('end', () => {
      response.writeHead(200, { 'Content-Type': 'application/json' });
      response.end(success);
    });
  });
  try {
    const client = new TranscriptionClient({
      // Settings probes accept the service root, `/v1`, or the complete endpoint. The
      // meeting egress must resolve all three to the same canonical endpoint exactly once.
      serviceUrl: `${direct.baseUrl}/v1/`,
      apiToken: 'operator-token',
      maxRetries: 0,
    });
    const result = await client.transcribe(pcm, 'en');
    check('direct in-cluster HTTP STT remains supported', result.text === 'hello');
    check('direct request reaches only the configured endpoint', directRequests === 1,
      `directRequests=${directRequests}`);
    check('a /v1 service URL is canonicalized without doubling the version path',
      directPath === '/v1/audio/transcriptions', `directPath=${JSON.stringify(directPath)}`);
    check('direct operator credential remains attached', directAuthorization === 'Bearer operator-token');
    check('direct request carries audio', directBytes > 0, `directBytes=${directBytes}`);
  } finally {
    await direct.close();
  }

  if (failed) {
    console.error(`\n❌ stt redirect policy: ${failed} check(s) FAILED.`);
    process.exit(1);
  }
  console.log('\n✅ stt redirect policy: redirect egress is refused; direct operator HTTP works.');
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
