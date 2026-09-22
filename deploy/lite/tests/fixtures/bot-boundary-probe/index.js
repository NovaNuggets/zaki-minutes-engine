const fs = require('node:fs');
const { execFileSync } = require('node:child_process');

function check(condition, message) {
  if (!condition) throw new Error(message);
}

check(process.getuid() !== 0, 'meeting bot still runs as root');
check(process.getgid() !== 0, 'meeting bot still has the root primary group');
const expectedUid = Number.parseInt(process.env.VEXA_LITE_BOT_UID ?? '', 10);
check(Number.isSafeInteger(expectedUid), 'trusted per-meeting uid is missing');
check(process.getuid() === expectedUid, 'wrong per-meeting bot uid');
check(execFileSync('id', ['-gn'], { encoding: 'utf8' }).trim() === 'vexa-bot', 'wrong bot group');
check(
  execFileSync('id', ['-nG'], { encoding: 'utf8' }).trim().split(/\s+/).includes('pulse-access'),
  'bot cannot access system PulseAudio',
);

const status = fs.readFileSync('/proc/self/status', 'utf8');
for (const capability of ['CapInh', 'CapPrm', 'CapEff', 'CapBnd', 'CapAmb']) {
  check(new RegExp(`^${capability}:\\s+0+$`, 'm').test(status), `${capability} was not cleared`);
}
check(/^NoNewPrivs:\s+1$/m.test(status), 'no_new_privs is not active');
check(process.env.DISPLAY === ':99', 'shared Xvfb display was not preserved');
check(process.env.HOME === `/var/lib/vexa-bot/${expectedUid}`, 'bot HOME was not isolated');

const tmpProbe = `/tmp/vexa-bot-boundary-${process.pid}`;
fs.writeFileSync(tmpProbe, 'ok', { mode: 0o600 });
fs.unlinkSync(tmpProbe);
fs.appendFileSync('/var/run/pulse/native', 'audio-access-ok');

for (const secretPath of [
  '/run/vexa/runtime-callback-secret',
  '/run/vexa/meeting-token-secret',
  '/run/vexa/admin-api-token',
  '/run/vexa/internal-api-secret',
  '/run/vexa/runtime-control-secret',
]) {
  let secretDenied = false;
  try {
    fs.readFileSync(secretPath);
  } catch (error) {
    secretDenied = error?.code === 'EACCES';
  }
  check(secretDenied, `bot could read root-only secret ${secretPath}`);
}

const mode = process.env.BOT_BOUNDARY_MODE ?? 'single';
const victimPidPath = '/tmp/vexa-bot-victim-pid';
const resultPath = '/tmp/vexa-bot-sibling-result';
const pause = () => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 25);
const waitFor = (path) => {
  const deadline = Date.now() + 5000;
  while (!fs.existsSync(path) && Date.now() < deadline) pause();
  check(fs.existsSync(path), `timed out waiting for ${path}`);
};

if (mode === 'victim') {
  fs.writeFileSync(victimPidPath, String(process.pid), { mode: 0o644 });
  waitFor(resultPath);
  check(fs.readFileSync(resultPath, 'utf8') === 'denied', 'sibling read the victim environment');
  console.log('bot-privilege sibling victim PASS');
} else if (mode === 'attacker') {
  waitFor(victimPidPath);
  const victimPid = fs.readFileSync(victimPidPath, 'utf8').trim();
  let denied = false;
  try {
    const victimEnvironment = fs.readFileSync(`/proc/${victimPid}/environ`);
    check(!victimEnvironment.includes(Buffer.from('victim-meeting-token')), 'stole sibling token');
  } catch (error) {
    denied = error?.code === 'EACCES' || error?.code === 'EPERM';
  }
  check(denied, 'sibling /proc environment was not kernel-denied');
  fs.writeFileSync(resultPath, 'denied', { mode: 0o644 });
  console.log('bot-privilege sibling attacker PASS');
} else {
  console.log('bot-privilege-permission probe PASS');
}
