/**
 * L-1699 regression guard: a Google Meet knock that Google refuses must end as
 * a denied join BEFORE any waiting-room evidence, while a genuine waiting room
 * (text indicator + Return button) still waits.
 *
 * Fixture replay of staging meeting 86: after "Ask to join", the page faulted
 * (DisconnectedError / EndCause) and only "Return to home screen" remained. The
 * pre-fix detector counted that button as waiting-room evidence, sent
 * awaiting_admission, and waited 600s. These fixtures pin the terminal denial,
 * the no-awaiting_admission invariant, the sanitized diagnostic line, the
 * genuine-lobby control, and the post-admission DisconnectedError negative.
 *
 * Run: npx tsx src/googlemeet/knock-refusal.test.ts
 */

import { setHooks } from "../_host";
import { AdmissionError } from "../shared/admission";
import { resetEscalation } from "../shared/escalation";
import {
  checkForGoogleWaitingRoomTextIndicators,
  waitForGoogleMeetingAdmission,
} from "./admission";
import { joinGoogleMeeting } from "./join";
import { markGoogleKnock, noteGooglePageFault } from "./knock";
import {
  googleNameInputSelectors,
  googleJoinButtonSelectors,
  googleAuthJoinCtaSelectors,
} from "./selectors";

let passed = 0, failed = 0;
function check(name: string, actual: unknown, expected: unknown) {
  const ok = actual === expected;
  console.log(`  ${ok ? "\x1b[32mPASS\x1b[0m" : "\x1b[31mFAIL\x1b[0m"}  ${name}`);
  if (!ok) console.log(`        expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  if (ok) passed++; else failed++;
}

const RETURN_BUTTON = 'button:has-text("Return to home screen")';
const WAITING_TEXT = 'text=Asking to be let in';
const SELF_NAME = '[data-self-name]';

// Optional fake clock for fixtures whose loops are bounded by Date.now().
// waitForTimeout advances it, so a 6s admission window costs no real time.
// Installed per-fixture; the pre-existing fixtures keep the real clock.
let fakeNow = 0;
let realNow: (() => number) | null = null;
function installFakeClock() {
  if (realNow) return;
  realNow = Date.now;
  fakeNow = realNow();
  Date.now = () => fakeNow;
}
function restoreRealClock() {
  if (!realNow) return;
  Date.now = realNow;
  realNow = null;
}

type Emitter = (arg: any) => void;

function mockPage(
  visible: string[],
  opts: {
    participantLabels?: string[];
    headline?: string;
    onWait?: (waits: number) => void;
    onClick?: (selector: string) => void;
  } = {},
): any {
  const state = { visible: new Set(visible) };
  const handlers = new Map<string, Emitter[]>();
  let waits = 0;
  const handle = (selector: string) => ({
    click: async () => { opts.onClick?.(selector); },
    isVisible: async () => true,
  });
  const page: any = {
    state,
    handlers,
    emit(event: string, arg: any) {
      for (const handler of handlers.get(event) ?? []) handler(arg);
    },
    locator: (selector: string) => ({
      first: () => ({
        isVisible: async () => state.visible.has(selector),
        getAttribute: async () => null,
      }),
      count: async () => (state.visible.has(selector) ? 1 : 0),
      evaluateAll: async () =>
        selector === '[data-participant-id]' ? (opts.participantLabels ?? []) : [],
    }),
    mouse: { move: async () => {} },
    frames: () => [],
    screenshot: async () => {},
    goto: async () => {},
    bringToFront: async () => {},
    fill: async () => {},
    waitForSelector: (selector: string) =>
      state.visible.has(selector)
        ? Promise.resolve(handle(selector))
        : Promise.reject(new Error(`mock: ${selector} absent`)),
    waitForTimeout: async (ms: number) => {
      waits += 1;
      if (realNow) fakeNow += ms;
      opts.onWait?.(waits);
    },
    evaluate: async () => opts.headline ?? '',
    url: () => 'https://meet.google.com/abc-defg-hij',
    on: (event: string, handler: Emitter) => {
      handlers.set(event, [...(handlers.get(event) ?? []), handler]);
    },
  };
  return page;
}

function emitDisconnectedError(page: any) {
  page.emit('console', {
    text: () => 'DisconnectedError, EndCause = 72, StartupCode = 200',
  });
}

function emitClientError(page: any) {
  page.emit('response', {
    status: () => 403,
    url: () => 'https://meet.google.com/abc-defg-hij?hs=123&authuser=0',
  });
}

function emitServerError(page: any) {
  page.emit('response', {
    status: () => 500,
    url: () => 'https://meet.google.com/abc-defg-hij/lobby/500?debug=1',
  });
}

async function captureLogs<T>(fn: () => Promise<T>): Promise<{ result: T; lines: string[] }> {
  const lines: string[] = [];
  const original = console.log;
  console.log = (...args: any[]) => {
    lines.push(args.map((arg) => String(arg)).join(' '));
  };
  try {
    return { result: await fn(), lines };
  } finally {
    console.log = original;
  }
}

async function main() {
  const states: string[] = [];
  setHooks({ onState: (state) => { states.push(state); } });

  console.log('\n=== L-1699: refused knock ends denied before waiting-room evidence ===');

  // 0. Evidence split: the Return button alone is not waiting-room proof, but
  //    waiting-room TEXT keeps the genuine lobby classification.
  check(
    'Return button alone is not waiting-room text evidence',
    await checkForGoogleWaitingRoomTextIndicators(mockPage([RETURN_BUTTON])),
    false,
  );
  check(
    'waiting text + Return button is waiting-room evidence',
    await checkForGoogleWaitingRoomTextIndicators(mockPage([WAITING_TEXT, RETURN_BUTTON])),
    true,
  );

  // 1. Meeting-86 replay: click -> page fault -> only Return button visible.
  {
    states.length = 0;
    const page = mockPage([RETURN_BUTTON], {
      headline: `You can't join meeting abc-defg-hij ${'x'.repeat(220)}`,
    });
    markGoogleKnock(page);
    emitDisconnectedError(page);
    emitClientError(page);
    emitServerError(page);

    const started = Date.now();
    const { result: error, lines } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 60_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });
    const elapsed = Date.now() - started;

    check('86 replay throws AdmissionError', error instanceof AdmissionError, true);
    check('86 replay outcome is denial', (error as AdmissionError)?.outcome, 'denial');
    check('86 replay ends inside the 60s bound', elapsed < 60_000, true);
    check('86 replay never sends awaiting_admission', states.includes('awaiting_admission'), false);

    const diagnostics = lines.filter((line) => line.includes('Google Meet refused the join knock'));
    check('86 replay emits exactly one refusal diagnostic', diagnostics.length, 1);
    const diagnostic = diagnostics[0] ?? '';
    check('diagnostic marks disconnected_error', diagnostic.includes('"reason":"disconnected_error"'), true);
    check('diagnostic redacts the meeting code in headline', diagnostic.includes('<code>'), true);
    check('diagnostic keeps headline within 200 chars', /"headline":"[^"]{201,}"/.test(diagnostic), false);
    check('diagnostic carries sanitized 4xx path', diagnostic.includes('"client_error_paths":["/<code>"]'), true);
    check('diagnostic omits non-4xx response paths', diagnostic.includes('/lobby/500'), false);
    check('diagnostic never leaks code or query', diagnostic.includes('abc-defg-hij') || diagnostic.includes('hs=123'), false);
  }

  // 2. Can't-join page copy is refusal evidence even without a console fault.
  {
    states.length = 0;
    const page = mockPage(["text=can't join this call"]);
    markGoogleKnock(page);

    const { result: error, lines } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 60_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });

    check('denied-text fixture throws AdmissionError', error instanceof AdmissionError, true);
    check('denied-text fixture outcome is denial', (error as AdmissionError)?.outcome, 'denial');
    check('denied-text fixture never sends awaiting_admission', states.includes('awaiting_admission'), false);

    const deniedDiagnostics = lines.filter((line) => line.includes('Google Meet refused the join knock'));
    check('denied-text fixture emits exactly one refusal diagnostic', deniedDiagnostics.length, 1);
    check(
      'denied-text diagnostic reason is denied_text',
      (deniedDiagnostics[0] ?? '').includes('"reason":"denied_text"'),
      true,
    );
  }

  // 3. Genuine waiting room: "Asking to be let in" + Return button still waits
  //    and reports awaiting_admission once, then admits when controls appear.
  {
    states.length = 0;
    let waits = 0;
    const page = mockPage([WAITING_TEXT, RETURN_BUTTON], {
      onWait: () => {
        waits += 1;
        if (waits === 1) {
          page.state.visible.delete(WAITING_TEXT);
          page.state.visible.add(SELF_NAME);
        }
      },
    });
    markGoogleKnock(page);

    const admitted = await waitForGoogleMeetingAdmission(page, 30_000, {});
    check('genuine waiting room admits after host let-in', admitted, true);
    check('genuine waiting room sent awaiting_admission once', states.filter((s) => s === 'awaiting_admission').length, 1);
  }

  // 4. Ordering guard: once waiting-room TEXT has been seen, a later page fault
  //    is a dropped lobby connection, not proof the original knock was refused.
  {
    states.length = 0;
    let waits = 0;
    const page = mockPage([WAITING_TEXT, RETURN_BUTTON], {
      onWait: () => {
        waits += 1;
        if (waits === 1) {
          page.state.visible.delete(WAITING_TEXT);
          page.state.visible.add(SELF_NAME);
        }
      },
    });
    markGoogleKnock(page);
    emitDisconnectedError(page);

    const admitted = await waitForGoogleMeetingAdmission(page, 30_000, {});
    check('post-waiting-text DisconnectedError still admits', admitted, true);
    check('post-waiting-text DisconnectedError is not a refusal', states.includes('awaiting_admission'), true);
  }

  // 5. Negative control: DisconnectedError after admission must not be
  //    reclassified as a refused knock.
  {
    states.length = 0;
    let waits = 0;
    const page = mockPage([], {
      onWait: () => {
        waits += 1;
        if (waits === 1) {
          page.state.visible.add(SELF_NAME);
          emitDisconnectedError(page);
        }
      },
    });
    markGoogleKnock(page);

    const admitted = await waitForGoogleMeetingAdmission(page, 30_000, {});
    check('post-admission DisconnectedError still admits', admitted, true);
    check('post-admission DisconnectedError is not a refusal', states.includes('awaiting_admission'), false);
  }

  // 6. The knock must be armed by join.ts itself: a refused click INSIDE
  //    joinGoogleMeeting (anonymous) ends denial with no awaiting_admission.
  //    The page is never armed by this test — only the product's own
  //    markGoogleKnock call sites can record the fault.
  {
    states.length = 0;
    resetEscalation();
    const nameInput = googleNameInputSelectors[0];
    const joinButton = googleJoinButtonSelectors[0];
    const page = mockPage([nameInput, joinButton], {
      onClick: (selector) => {
        if (selector === joinButton) {
          page.state.visible = new Set([RETURN_BUTTON]);
          emitDisconnectedError(page);
          emitClientError(page);
        }
      },
    });

    await joinGoogleMeeting(page, 'https://meet.google.com/abc-defg-hij', 'ZAKI Notetaker', {
      platform: 'google_meet',
      authenticated: false,
      uiInteractionMode: 'synthetic',
    } as any);

    const { result: error, lines } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 60_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });

    check('anonymous join refusal throws AdmissionError', error instanceof AdmissionError, true);
    check('anonymous join refusal outcome is denial', (error as AdmissionError)?.outcome, 'denial');
    check('anonymous join refusal never sends awaiting_admission', states.includes('awaiting_admission'), false);
    const anonDiagnostics = lines.filter((line) => line.includes('Google Meet refused the join knock'));
    check('anonymous join refusal emits exactly one diagnostic', anonDiagnostics.length, 1);
  }

  // 7. Same pin through the authenticated CTA: join.ts arms the knock before
  //    the click, so a fault inside the click still ends denial.
  {
    states.length = 0;
    resetEscalation();
    const cta = googleAuthJoinCtaSelectors[0];
    const page = mockPage([cta], {
      onClick: (selector) => {
        if (selector === cta) {
          page.state.visible = new Set([RETURN_BUTTON]);
          emitDisconnectedError(page);
        }
      },
    });

    await joinGoogleMeeting(page, 'https://meet.google.com/abc-defg-hij', 'ZAKI Notetaker', {
      platform: 'google_meet',
      authenticated: true,
      uiInteractionMode: 'synthetic',
    } as any);

    const { result: error } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 60_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });

    check('authenticated join refusal throws AdmissionError', error instanceof AdmissionError, true);
    check('authenticated join refusal outcome is denial', (error as AdmissionError)?.outcome, 'denial');
    check('authenticated join refusal never sends awaiting_admission', states.includes('awaiting_admission'), false);
  }

  // 8. Fault lands in the LAST wait of the polling window: the final
  //    admission check still classifies the refused knock as denial, with
  //    exactly one diagnostic.
  {
    states.length = 0;
    resetEscalation();
    installFakeClock();
    const page = mockPage([], {
      onWait: (waits) => {
        if (waits === 3) emitDisconnectedError(page);
      },
    });
    markGoogleKnock(page);

    const { result: error, lines } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 6_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });
    restoreRealClock();

    check('final-boundary fault throws AdmissionError', error instanceof AdmissionError, true);
    check('final-boundary fault outcome is denial', (error as AdmissionError)?.outcome, 'denial');
    const finalDiagnostics = lines.filter((line) => line.includes('Google Meet refused the join knock'));
    check('final-boundary fault emits exactly one diagnostic', finalDiagnostics.length, 1);
  }

  // 9. Per-knock reset: a faulted knock followed by a FRESH markGoogleKnock on
  //    the same page must not keep the stale fault — no new fault means the
  //    page times out as a join failure, never a denial.
  {
    states.length = 0;
    resetEscalation();
    installFakeClock();
    const page = mockPage([RETURN_BUTTON]);
    markGoogleKnock(page);
    emitDisconnectedError(page);
    markGoogleKnock(page);

    const { result: error, lines } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 6_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });
    restoreRealClock();

    check('re-knock after fault throws AdmissionError', error instanceof AdmissionError, true);
    check('re-knock after fault outcome is join_failure', (error as AdmissionError)?.outcome, 'join_failure');
    const reknotDiagnostics = lines.filter((line) => line.includes('Google Meet refused the join knock'));
    check('re-knock after fault emits no diagnostic', reknotDiagnostics.length, 0);
  }

  // 10. Waiting-room TEXT seen, then a fault, text stays to timeout: a lobby
  //     the host never answered is lobby_timeout, never denial.
  {
    states.length = 0;
    resetEscalation();
    installFakeClock();
    const page = mockPage([], {
      onWait: (waits) => {
        if (waits === 1) page.state.visible.add(WAITING_TEXT);
        if (waits === 2) emitDisconnectedError(page);
      },
    });
    markGoogleKnock(page);

    const { result: error } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 6_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });
    restoreRealClock();

    check('lobby-then-fault throws AdmissionError', error instanceof AdmissionError, true);
    check('lobby-then-fault outcome is lobby_timeout', (error as AdmissionError)?.outcome, 'lobby_timeout');
    check('lobby-then-fault sent awaiting_admission once', states.filter((s) => s === 'awaiting_admission').length, 1);
  }

  // 11. Waiting-room TEXT seen first, then it disappears with a fault and the
  //     bot is never admitted: join_failure, never denial.
  {
    states.length = 0;
    resetEscalation();
    installFakeClock();
    const page = mockPage([WAITING_TEXT], {
      onWait: (waits) => {
        if (waits === 1) {
          page.state.visible.delete(WAITING_TEXT);
          emitDisconnectedError(page);
        }
      },
    });
    markGoogleKnock(page);

    const { result: error, lines } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 6_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });
    restoreRealClock();

    check('text-gone-with-fault throws AdmissionError', error instanceof AdmissionError, true);
    check('text-gone-with-fault outcome is join_failure', (error as AdmissionError)?.outcome, 'join_failure');
    const goneDiagnostics = lines.filter((line) => line.includes('Google Meet refused the join knock'));
    check('text-gone-with-fault emits no diagnostic', goneDiagnostics.length, 0);
  }

  // 12. The embedder's page-fault relay (noteGooglePageFault): ONE relayed
  //     `[page-fault] error:` line with another code than 72 refuses the
  //     knock. The page emits no console/pageerror event at all.
  {
    states.length = 0;
    resetEscalation();
    installFakeClock();
    const page = mockPage([RETURN_BUTTON]);
    markGoogleKnock(page);
    noteGooglePageFault(
      page,
      '[page-fault] error: CustomError: Error in protected function: DisconnectedError, EndCause = 5, StartupCode = 200 :: DisconnectedError: …',
    );

    const { result: error, lines } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 6_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });
    restoreRealClock();

    check('relayed page fault outcome is denial', (error as AdmissionError)?.outcome, 'denial');
    const relayedDiagnostics = lines.filter((line) => line.includes('Google Meet refused the join knock'));
    check('relayed page fault emits exactly one diagnostic', relayedDiagnostics.length, 1);
    check('relayed page fault diagnostic reason is disconnected_error', (relayedDiagnostics[0] ?? '').includes('"reason":"disconnected_error"'), true);
    check('relayed page fault never sends awaiting_admission', states.includes('awaiting_admission'), false);
  }

  // 13. Fixture 11 on the relay route: waiting-room TEXT seen, then gone, then
  //     a relayed fault, never admitted: join_failure, never denial.
  {
    states.length = 0;
    resetEscalation();
    installFakeClock();
    const page = mockPage([WAITING_TEXT], {
      onWait: (waits) => {
        if (waits === 1) page.state.visible.delete(WAITING_TEXT);
        if (waits === 2) {
          noteGooglePageFault(
            page,
            '[page-fault] unhandledrejection: DisconnectedError: DisconnectedError, EndCause = 72, StartupCode = 200 :: …',
          );
        }
      },
    });
    markGoogleKnock(page);

    const { result: error, lines } = await captureLogs(async () => {
      try {
        await waitForGoogleMeetingAdmission(page, 6_000, {});
        return null;
      } catch (err) {
        return err;
      }
    });
    restoreRealClock();

    check('text-gone-then-relayed-fault outcome is join_failure', (error as AdmissionError)?.outcome, 'join_failure');
    const relayGoneDiagnostics = lines.filter((line) => line.includes('Google Meet refused the join knock'));
    check('text-gone-then-relayed-fault emits no diagnostic', relayGoneDiagnostics.length, 0);
  }

  setHooks({});

  console.log(`\n=== summary: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
