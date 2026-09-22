/** Recording upload transport must not replay MeetingTokens or reflect response bodies. */
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { once } from 'node:events';
import { RecordingService } from './recording';

let failed = 0;
const check = (name: string, condition: boolean, detail = '') => {
  console.log(`  ${condition ? '✅' : '❌'} ${name}${condition ? '' : ` — ${detail}`}`);
  if (!condition) failed++;
};

async function serve(handler: (request: IncomingMessage, response: ServerResponse) => void) {
  const server = createServer(handler);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('test server has no TCP address');
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: async () => { server.close(); await once(server, 'close'); },
  };
}

async function send(service: RecordingService, url: string): Promise<Error | undefined> {
  try {
    await (service as unknown as {
      _sendUpload(url: string, token: string, boundary: string, body: Buffer, timeout: number): Promise<void>;
    })._sendUpload(url, 'RECORDING-TOKEN', 'safe-boundary', Buffer.from('audio'), 1_000);
    return undefined;
  } catch (error) {
    return error as Error;
  }
}

async function main(): Promise<void> {
  let sinkRequests = 0;
  const sink = await serve((_request, response) => {
    sinkRequests++;
    response.writeHead(204).end();
  });
  const redirect = await serve((_request, response) => {
    response.writeHead(307, { location: `${sink.url}/credential-sink` }).end();
  });
  const hostile = await serve((_request, response) => {
    response.writeHead(500, { 'content-type': 'text/plain' });
    response.end(`upstream echoed Bearer RECORDING-TOKEN ${'x'.repeat(128 * 1024)}`);
  });

  try {
    const service = new RecordingService(7, 'session');
    const redirected = await send(service, `${redirect.url}/upload`);
    check('redirect response is rejected', redirected instanceof Error);
    check('redirect target receives no recording or token', sinkRequests === 0, String(sinkRequests));

    const rejected = await send(service, `${hostile.url}/upload`);
    check('non-2xx response is rejected', rejected instanceof Error);
    check('response body and reflected token never enter the error',
      !String(rejected?.message).includes('RECORDING-TOKEN') && String(rejected?.message).length < 256,
      String(rejected?.message));

    const unsafe = await send(service, 'file:///tmp/credential-sink');
    check('non-http upload scheme is rejected', unsafe instanceof Error);
  } finally {
    await redirect.close();
    await hostile.close();
    await sink.close();
  }

  if (failed) process.exit(1);
  console.log('\n✅ recording transport: no redirects, bounded opaque failures, HTTP(S) only.');
}

void main();
