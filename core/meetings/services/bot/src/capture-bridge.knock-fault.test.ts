/**
 * L-1699 c — a Google Meet refusal that reaches the bot ONLY through the page-fault bridge must
 * end the knock as `denial`, exactly like the same fault on Playwright's `console`/`pageerror`.
 *
 * launchBrowser installs a document-start listener for window `error`/`unhandledrejection` in
 * every frame and relays each as `[page-fault] …` through the exposed `logBot` binding. On staging
 * meeting 92 (2026-11-23) Google's refused knock (`DisconnectedError, EndCause = 72`) arrived on
 * that route alone: 15 `[page] [page-fault]` lines (8 `error`, 7 `unhandledrejection`), 0
 * `[page-console:…]` lines, no `pageerror`. The click was at 09.234, the admission check started at
 * 10.022, the faults landed at 10.301–10.350, and the bot then polled the whole window.
 *
 * These fixtures run the REAL launchBrowser with its one Chromium launch
 * (@vexa/remote-browser → playwright-extra `launchPersistentContext`) answered by a fake context,
 * so the bridge's own init script and the `logBot` handler it registers are captured verbatim. The
 * init script runs against a fake frame `window`; the 11-23 events are dispatched into it and reach
 * Node through the captured handler on a later event-loop turn, as a Playwright binding call does.
 * @vexa/join's real joinMeeting drives the anonymous Meet join and the admission wait.
 *
 *   1  bridge route only, fault inside the "Ask to join" click
 *   3  bridge route only, the 11-23 order: ~1 s after the click, during the first admission check
 *   3c bridge route only, 11-23 order, ONE event: a single `error`, a single `unhandledrejection`,
 *      and a single `error` with another code (`EndCause = 5`)
 *   3b the same fault at the same moment through Playwright `console` only, then `pageerror` only
 *   N  bridged lines without `DisconnectedError` or without `EndCause =` are not a refusal
 *   W  waiting-room text seen first, then gone, then the bridge fault: never a refusal (#73's rule)
 *
 * Run: npx tsx src/capture-bridge.knock-fault.test.ts
 */
import { createRequire } from 'node:module';
import vm from 'node:vm';
import { joinMeeting, AdmissionError, type JoinState } from '@vexa/join';
import { launchBrowser } from './capture-bridge.js';
import type { Invocation } from './config.js';

let passed = 0, failed = 0;
function check(name: string, actual: unknown, expected: unknown): void {
  const ok = actual === expected;
  console.log(`  ${ok ? '\x1b[32mPASS\x1b[0m' : '\x1b[31mFAIL\x1b[0m'}  ${name}`);
  if (!ok) console.log(`        expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  if (ok) passed++; else failed++;
}

// The bridge's one launch call, resolved from the module instance @vexa/remote-browser itself loads.
const { chromium } = createRequire(import.meta.resolve('@vexa/remote-browser'))('playwright-extra');

// Fake clock: the admission loops are bounded by Date.now(); the mock's waitForTimeout advances it.
const realNow = Date.now;
let fakeNow = realNow();
Date.now = () => fakeNow;

/** One event-loop turn: how a Playwright binding call or page event reaches Node. */
const later = (fn: () => void) => setImmediate(fn);

const NAME_INPUT = 'input[type="text"][aria-label="Your name"]';
const ASK_TO_JOIN = 'button:has-text("Ask to join")';
const RETURN_BUTTON = 'button:has-text("Return to home screen")';
const WAITING_TEXT = 'text=Asking to be let in';
const SELF_NAME = '[data-self-name]';
const POLL_INTERVAL_MS = 2_000;   // waitForGoogleMeetingAdmission's checkInterval: one poll cycle
const REFUSAL_DIAGNOSTIC = 'Google Meet refused the join knock';

const FAULT = 'DisconnectedError, EndCause = 72, StartupCode = 200';
// The two 11-23 page-side events, as the bridge receives them (stacks cut).
const errorEvent = () => ({
  type: 'error', isTrusted: true,
  error: { name: 'CustomError', message: `Error in protected function: ${FAULT}`, stack: `DisconnectedError: ${FAULT}\n    at <cut>` },
});
const rejectionEvent = () => ({
  type: 'unhandledrejection', isTrusted: true,
  reason: { name: 'DisconnectedError', message: FAULT, stack: `DisconnectedError: ${FAULT}\n    at <cut>` },
});

interface Scenario {
  /** "Ask to join" leaves this on screen (the 11-23 refusal left only the Return button). */
  afterClick: string[];
  /** Fires inside the "Ask to join" click. */
  onClick?: () => void;
  /** Fires in the first admission check after the click, ~1 s after it (the 11-23 order). */
  onFirstAdmissionCheck?: () => void;
  /** Fires after each page wait that follows the click; 1 = the first admission poll's wait. */
  onWait?: (waitsSinceClick: number) => void;
}

function meetPage(s: Scenario): any {
  let visible = new Set([NAME_INPUT, ASK_TO_JOIN]);
  const handlers = new Map<string, ((arg: any) => void)[]>();
  let clickedAt: number | null = null;
  let probed = false;
  let waits = 0;
  const page: any = {
    playwrightEvents: 0,
    show: (selectors: string[]) => { visible = new Set(selectors); },
    hasShown: (selector: string) => visible.has(selector),
    emit(event: string, arg: any) {
      page.playwrightEvents++;
      for (const handler of handlers.get(event) ?? []) handler(arg);
    },
    on(event: string, handler: (arg: any) => void) {
      handlers.set(event, [...(handlers.get(event) ?? []), handler]);
    },
    goto: async () => {},
    bringToFront: async () => {},
    screenshot: async () => {},
    fill: async () => {},
    url: () => 'https://meet.google.com/abc-defg-hij',
    frames: () => [],
    evaluate: async () => "You can't join this video call",
    waitForSelector: async (selector: string) => {
      if (!visible.has(selector)) throw new Error(`mock: ${selector} absent`);
      return {
        isVisible: async () => true,
        click: async () => {
          if (selector !== ASK_TO_JOIN) return;
          clickedAt = fakeNow;
          waits = 0;
          visible = new Set(s.afterClick);
          s.onClick?.();
        },
      };
    },
    waitForTimeout: async (ms: number) => {
      fakeNow += ms;
      await new Promise<void>((resolve) => setImmediate(resolve));   // the real wait yields the loop
      if (clickedAt !== null) s.onWait?.(++waits);
    },
    mouse: {
      // The admission indicator check moves the pointer; its first call after the click is the
      // first admission check.
      move: async () => {
        if (clickedAt === null || probed) return;
        probed = true;
        fakeNow = Math.max(fakeNow, clickedAt + 1_070);
        s.onFirstAdmissionCheck?.();
      },
    },
    locator: (selector: string) => ({
      first: () => ({
        isVisible: async () => visible.has(selector),
        getAttribute: async () => null,
      }),
      count: async () => (visible.has(selector) ? 1 : 0),
      evaluateAll: async () => [],
    }),
  };
  return page;
}

/** launchBrowser on a fake context: returns the bridge's page-fault init script, run in one
 *  frame per call, wired to the logBot handler launchBrowser registered. */
async function bridgeFor(page: any) {
  const initScripts: unknown[] = [];
  let logBot: ((m: string) => unknown) | null = null;
  chromium.launchPersistentContext = async () => ({
    pages: () => [page],
    newPage: async () => page,
    addInitScript: async (script: unknown) => { initScripts.push(script); },
    exposeFunction: async (name: string, fn: (m: string) => unknown) => { if (name === 'logBot') logBot = fn; },
    close: async () => {},
  });
  const session = await launchBrowser({ platform: 'google_meet', authenticated: false } as unknown as Invocation);
  const script = initScripts.find((s) => typeof s === 'string' && s.includes("'[page-fault] '")) as string | undefined;
  check('launchBrowser installs the page-fault bridge script', typeof script, 'string');
  check('launchBrowser registers a logBot handler', typeof logBot, 'function');
  check('launchBrowser hands the join layer the page the bridge watches', session.page, page);

  const consoleFallback: unknown[] = [];
  const frame = () => {
    const listeners = new Map<string, ((ev: any) => void)[]>();
    const window = {
      // A Playwright binding call: async, handled Node-side on a later turn.
      logBot: (m: string) => new Promise((resolve) => later(() => resolve(logBot!(m)))),
      addEventListener: (type: string, fn: (ev: any) => void) => {
        listeners.set(type, [...(listeners.get(type) ?? []), fn]);
      },
    };
    vm.runInNewContext(script ?? '', { window, console: { error: (...a: unknown[]) => consoleFallback.push(a) } });
    return (type: string, ev: unknown) => { for (const fn of listeners.get(type) ?? []) fn(ev); };
  };
  return { session, frame, consoleFallback };
}

interface Run { error: unknown; admitted: boolean | null; lines: string[]; states: JoinState[]; endedAt: number }

async function runJoin(page: any): Promise<Run> {
  const states: JoinState[] = [];
  const lines: string[] = [];
  const original = console.log;
  console.log = (...args: unknown[]) => { lines.push(args.map(String).join(' ')); };
  let error: unknown = null;
  let admitted: boolean | null = null;
  try {
    const r = await joinMeeting(page, {
      meetingUrl: 'https://meet.google.com/abc-defg-hij',
      platform: 'google_meet',
      botName: 'ZAKI Notetaker',
      authenticated: false,
      uiInteractionMode: 'synthetic',
      waitingRoomTimeoutMs: 60_000,
      hooks: { onState: (state: JoinState) => { states.push(state); } },
    });
    admitted = r.admitted;
  } catch (e) {
    error = e;
  } finally {
    console.log = original;
  }
  return { error, admitted, lines, states, endedAt: fakeNow };
}

const diagnosticsIn = (run: Run) => run.lines.filter((l) => l.includes(REFUSAL_DIAGNOSTIC));
const outcomeOf = (run: Run) => (run.error instanceof AdmissionError ? run.error.outcome : `not an AdmissionError: ${String(run.error)}`);

function expectRefused(label: string, run: Run): void {
  check(`${label}: outcome is denial`, outcomeOf(run), 'denial');
  check(`${label}: exactly one refusal diagnostic`, diagnosticsIn(run).length, 1);
  check(`${label}: diagnostic reason is disconnected_error`, (diagnosticsIn(run)[0] ?? '').includes('"reason":"disconnected_error"'), true);
  check(`${label}: never sends awaiting_admission`, run.states.filter((s) => s === 'awaiting_admission').length, 0);
}

async function main(): Promise<void> {
  console.log('\n=== L-1699 c: a refused Meet knock that arrives only through the page-fault bridge ===');

  // 1. Bridge route only, fault inside the "Ask to join" click.
  {
    const page = meetPage({ afterClick: [RETURN_BUTTON], onClick: () => fire() });
    const { session, frame, consoleFallback } = await bridgeFor(page);
    const dispatch = frame();
    function fire() {
      dispatch('error', errorEvent());
      dispatch('unhandledrejection', rejectionEvent());
    }
    const run = await runJoin(page);
    expectRefused('1 bridge, in the click', run);
    check('1 bridge, in the click: no Playwright console/pageerror event was emitted', page.playwrightEvents, 0);
    check('1 bridge, in the click: the bridge used logBot, not its console fallback', consoleFallback.length, 0);
    check(
      '1 bridge, in the click: the bridge still logs both [page-fault] lines',
      run.lines.filter((l) => l.startsWith('[page] [page-fault] ')).length,
      2,
    );
    check(
      '1 bridge, in the click: the logged line keeps the 11-23 error shape',
      run.lines.some((l) => l.startsWith(`[page] [page-fault] error: CustomError: Error in protected function: ${FAULT} :: DisconnectedError: `)),
      true,
    );
    await session.close();
  }

  // 3. Bridge route only, the 11-23 order: the click returns, the admission wait starts, and the
  //    15 faults (8 error + 7 unhandledrejection) land ~1 s after the click, in a child frame,
  //    during the first admission check.
  {
    let faultAt = 0;
    const page = meetPage({ afterClick: [RETURN_BUTTON], onFirstAdmissionCheck: () => fire() });
    const { session, frame, consoleFallback } = await bridgeFor(page);
    frame();                     // the main frame's bridge instance stays quiet
    const childFrame = frame();
    function fire() {
      faultAt = fakeNow;
      for (let i = 0; i < 15; i++) {
        if (i % 2 === 0) childFrame('error', errorEvent());
        else childFrame('unhandledrejection', rejectionEvent());
      }
    }
    const run = await runJoin(page);
    expectRefused('3 bridge, 11-23 order', run);
    check('3 bridge, 11-23 order: the fault landed after the click, in the admission check', faultAt > 0, true);
    check('3 bridge, 11-23 order: denial within one poll cycle of the fault', run.endedAt - faultAt <= POLL_INTERVAL_MS, true);
    check('3 bridge, 11-23 order: no Playwright console/pageerror event was emitted', page.playwrightEvents, 0);
    check('3 bridge, 11-23 order: the bridge used logBot, not its console fallback', consoleFallback.length, 0);
    check(
      '3 bridge, 11-23 order: the bridge still logs all 15 [page-fault] lines',
      run.lines.filter((l) => l.startsWith('[page] [page-fault] ')).length,
      15,
    );
    check('3 bridge, 11-23 order: no [page-console:…] line carries the fault', run.lines.filter((l) => l.startsWith('[page-console:')).length, 0);
    await session.close();
  }

  // 3c. Bridge route only, the 11-23 order, ONE event of ONE type: either event alone is a refusal,
  //     and so is any `EndCause =` code, not just 72.
  for (const [label, type, event] of [
    ['3c bridge, one error event', 'error', errorEvent()],
    ['3c bridge, one unhandledrejection event', 'unhandledrejection', rejectionEvent()],
    ['3c bridge, one error event with EndCause = 5', 'error', {
      type: 'error', isTrusted: true,
      error: { name: 'CustomError', message: 'Error in protected function: DisconnectedError, EndCause = 5, StartupCode = 200', stack: 'DisconnectedError: DisconnectedError, EndCause = 5, StartupCode = 200\n    at <cut>' },
    }],
  ] as const) {
    let faultAt = 0;
    const page = meetPage({ afterClick: [RETURN_BUTTON], onFirstAdmissionCheck: () => { faultAt = fakeNow; dispatch(type, event); } });
    const { session, frame } = await bridgeFor(page);
    const dispatch = frame();
    const run = await runJoin(page);
    expectRefused(label, run);
    check(`${label}: denial within one poll cycle of the fault`, run.endedAt - faultAt <= POLL_INTERVAL_MS, true);
    check(`${label}: exactly one [page-fault] line crossed the bridge`, run.lines.filter((l) => l.startsWith('[page] [page-fault] ')).length, 1);
    check(`${label}: no Playwright console/pageerror event was emitted`, page.playwrightEvents, 0);
    await session.close();
  }

  // 3b. The same fault at the same moment through Playwright's `console` only — then `pageerror`
  //     only. The bridge is wired but nothing is dispatched into it.
  {
    let faultAt = 0;
    const page = meetPage({
      afterClick: [RETURN_BUTTON],
      onFirstAdmissionCheck: () => {
        faultAt = fakeNow;
        later(() => page.emit('console', { type: () => 'error', text: () => `Error in protected function: ${FAULT}` }));
      },
    });
    const { session } = await bridgeFor(page);
    const run = await runJoin(page);
    expectRefused('3b console only, 11-23 order', run);
    check('3b console only, 11-23 order: denial within one poll cycle of the fault', run.endedAt - faultAt <= POLL_INTERVAL_MS, true);
    check('3b console only, 11-23 order: nothing crossed the bridge', run.lines.filter((l) => l.startsWith('[page] [page-fault] ')).length, 0);
    await session.close();
  }
  {
    let faultAt = 0;
    const page = meetPage({
      afterClick: [RETURN_BUTTON],
      onFirstAdmissionCheck: () => {
        faultAt = fakeNow;
        later(() => page.emit('pageerror', Object.assign(new Error(FAULT), { name: 'DisconnectedError' })));
      },
    });
    const { session } = await bridgeFor(page);
    const run = await runJoin(page);
    expectRefused('3b pageerror only, 11-23 order', run);
    check('3b pageerror only, 11-23 order: denial within one poll cycle of the fault', run.endedAt - faultAt <= POLL_INTERVAL_MS, true);
    check('3b pageerror only, 11-23 order: nothing crossed the bridge', run.lines.filter((l) => l.startsWith('[page] [page-fault] ')).length, 0);
    await session.close();
  }

  // N. Negative control: bridged lines that carry `DisconnectedError` without `EndCause =`, or
  //    `EndCause =` without `DisconnectedError`, are not a refusal. They cross during the first
  //    poll's wait; the second poll still sees no lobby text, so a refusal recorded from them
  //    would end the knock as denial there. The waiting room shows after that wait and the
  //    host admits after the next one.
  {
    const page = meetPage({
      afterClick: [],
      onFirstAdmissionCheck: () => {
        dispatch('error', { type: 'error', error: { name: 'DisconnectedError', message: 'DisconnectedError', stack: 'DisconnectedError\n    at <cut>' } });
        dispatch('unhandledrejection', { type: 'unhandledrejection', reason: { name: 'Error', message: 'EndCause = 72', stack: '' } });
      },
      onWait: (waits) => {
        if (waits === 2) page.show([WAITING_TEXT]);
        if (waits === 3) page.show([SELF_NAME]);
      },
    });
    const { session, frame } = await bridgeFor(page);
    const dispatch = frame();
    const run = await runJoin(page);
    check('N partial fault lines: both crossed the bridge', run.lines.filter((l) => l.startsWith('[page] [page-fault] ')).length, 2);
    check('N partial fault lines: no AdmissionError', outcomeOf(run), 'not an AdmissionError: null');
    check('N partial fault lines: the knock is admitted', run.admitted, true);
    check('N partial fault lines: no refusal diagnostic', diagnosticsIn(run).length, 0);
    check('N partial fault lines: awaiting_admission sent once', run.states.filter((s) => s === 'awaiting_admission').length, 1);
    await session.close();
  }

  // The relayed argument crosses from the page: a non-string must never throw in the handler.
  {
    const page = meetPage({ afterClick: [] });
    let logBot: ((m: unknown) => unknown) | null = null;
    chromium.launchPersistentContext = async () => ({
      pages: () => [page], newPage: async () => page,
      addInitScript: async () => {},
      exposeFunction: async (name: string, fn: (m: unknown) => unknown) => { if (name === 'logBot') logBot = fn; },
      close: async () => {},
    });
    const session = await launchBrowser({ platform: 'google_meet', authenticated: false } as unknown as Invocation);
    const original = console.log;
    console.log = () => {};
    let threw: unknown = null;
    try { logBot!(undefined); logBot!({ m: 1 }); } catch (e) { threw = e; } finally { console.log = original; }
    check('logBot handler accepts a non-string page argument', threw === null ? 'no throw' : String(threw), 'no throw');
    await session.close();
  }

  // W. #73's waiting-room-first rule on the bridge route: the click shows waiting-room text, the
  //    text is gone after the first wait (never admitted), and only then does the 11-23 fault cross
  //    the bridge. A dropped waiting room is not a refused knock: join_failure, no diagnostic.
  //    Runs last: it waits out the window, which arms the join layer's escalation extension.
  {
    const page = meetPage({
      afterClick: [WAITING_TEXT],
      onWait: (waits) => {
        if (waits === 1) page.show([]);
        if (waits === 2) {
          dispatch('error', errorEvent());
          dispatch('unhandledrejection', rejectionEvent());
        }
      },
    });
    const { session, frame } = await bridgeFor(page);
    const dispatch = frame();
    const run = await runJoin(page);
    check('W lobby, then gone, then bridge fault: both lines crossed the bridge', run.lines.filter((l) => l.startsWith('[page] [page-fault] ')).length, 2);
    check('W lobby, then gone, then bridge fault: outcome is join_failure', outcomeOf(run), 'join_failure');
    check('W lobby, then gone, then bridge fault: no refusal diagnostic', diagnosticsIn(run).length, 0);
    check('W lobby, then gone, then bridge fault: awaiting_admission sent once', run.states.filter((s) => s === 'awaiting_admission').length, 1);
    await session.close();
  }

  Date.now = realNow;
  console.log(`\n=== summary: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((error) => {
  Date.now = realNow;
  console.error(error);
  process.exit(1);
});
