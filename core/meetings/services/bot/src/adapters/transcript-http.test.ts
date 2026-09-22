import { createHttpTranscriptSink, type ManagedFetchLike } from './transcript-http.js';
import { TranscriptWriteFenced } from './transcript-redis.js';
import type { TranscriptSegment } from '../contracts.js';

let failed = 0;
const check = (name: string, condition: boolean, detail = '') => {
  console.log(`  ${condition ? '✅' : '❌'} ${name}${condition ? '' : ` — ${detail}`}`);
  if (!condition) failed++;
};
const segment: TranscriptSegment = {
  segment_id: 'sess:speaker:1000', speaker: 'Ada', text: 'Launch review',
  start: 1, end: 2, completed: true, language: 'en',
};

async function main(): Promise<void> {
  const calls: Array<{ url: string; init: Parameters<ManagedFetchLike>[1] }> = [];
  const fetchImpl: ManagedFetchLike = async (url, init) => {
    calls.push({ url, init });
    return { ok: true, status: 202 };
  };
  const sink = createHttpTranscriptSink({
    transcriptIngestUrl: 'http://meeting-api:8080/bots/internal/transcripts/ingest',
    retentionFenceUrl: 'http://meeting-api:8080/bots/internal/transcripts/fence',
    token: 'MEETING-TOKEN', connectionId: 'sess', fetchImpl, sleep: async () => {},
  });
  await sink.publish(segment);
  check('publish uses the mediated URL', calls[0]?.url.endsWith('/bots/internal/transcripts/ingest') === true);
  check('publish refuses redirects', calls[0]?.init.redirect === 'error');
  check('publish carries only the MeetingToken bearer', calls[0]?.init.headers.authorization === 'Bearer MEETING-TOKEN');
  check('publish has no platform secret header', !('x-internal-secret' in (calls[0]?.init.headers ?? {})));
  check('body contains session + segment but no meeting selector', (() => {
    const body = JSON.parse(calls[0]!.init.body);
    return body.connection_id === 'sess' && body.segment.segment_id === segment.segment_id && body.meeting_id === undefined;
  })());

  await sink.fenceMeetingCarriers({ fenceKey: 'ignored', raw: true, processed: true });
  check('deadline uses the mediated fence URL', calls[1]?.url.endsWith('/bots/internal/transcripts/fence') === true);

  let attempts = 0;
  const retry = createHttpTranscriptSink({
    transcriptIngestUrl: 'http://meeting-api/ingest', retentionFenceUrl: 'http://meeting-api/fence',
    token: 't', connectionId: 's', sleep: async () => {},
    fetchImpl: async () => (++attempts === 1 ? { ok: false, status: 503 } : { ok: true, status: 202 }),
  });
  await retry.publish(segment);
  check('transient failure is retried within a bound', attempts === 2, String(attempts));

  const fenced = createHttpTranscriptSink({
    transcriptIngestUrl: 'http://meeting-api/ingest', retentionFenceUrl: 'http://meeting-api/fence',
    token: 't', connectionId: 's', sleep: async () => {},
    fetchImpl: async () => ({ ok: false, status: 409 }),
  });
  let wasFenced = false;
  try { await fenced.publish(segment); } catch (error) { wasFenced = error instanceof TranscriptWriteFenced; }
  check('remote retention refusal trips the local fence error', wasFenced);

  if (failed) process.exit(1);
  console.log('\n✅ transcript-http: managed v2 posts bounded token-bound segments without Redis authority.');
}

void main();

