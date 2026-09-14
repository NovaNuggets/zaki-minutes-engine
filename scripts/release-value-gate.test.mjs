// Unit tests for release-value-gate.mjs: the per-PR batch verdict (guarantee line 8).
// Run: node --test scripts/release-value-gate.test.mjs
//
// DEC-2026-09-14-7 (D9 retired): `state: value-signed` is no longer a term. A non-runtime change
// counts as accepted on the same diff acceptance the merge card required at merge — a fresh
// non-author approval (or a maintainer author) — and on nothing else. A RED value-fsm is still
// never waived, and a runtime PR with no value-fsm run still fails closed.

import test from "node:test";
import assert from "node:assert/strict";
import { batchVerdict } from "./release-value-gate.mjs";

test("D9 retired: labelless non-runtime merged PR with a non-author approval → ACCEPTED", () => {
  const r = batchVerdict({ runtime: false, vf: "absent", diffOk: true });
  assert.equal(r.verdict, "ACCEPTED");
  assert.match(r.why, /diff accepted at merge/);
  assert.doesNotMatch(r.why, /value-signed/); // the label is gone from the basis line
});

test("non-runtime with NO approval on its merged head → UNACCEPTED (the bar was not met)", () => {
  assert.equal(batchVerdict({ runtime: false, vf: "absent", diffOk: false }).verdict, "UNACCEPTED");
});

test("value-fsm green → ACCEPTED regardless of classification", () => {
  assert.equal(batchVerdict({ runtime: true, vf: "success", diffOk: false }).verdict, "ACCEPTED");
  assert.equal(batchVerdict({ runtime: false, vf: "success", diffOk: false }).verdict, "ACCEPTED");
});

test("value-fsm RED → UNACCEPTED, and nothing waives it", () => {
  for (const runtime of [true, false]) {
    const r = batchVerdict({ runtime, vf: "failure", diffOk: true });
    assert.equal(r.verdict, "UNACCEPTED");
    assert.match(r.why, /nothing waives it/);
  }
});

test("runtime PR with no value-fsm run → UNACCEPTED (fails closed)", () => {
  assert.equal(batchVerdict({ runtime: true, vf: "absent", diffOk: true }).verdict, "UNACCEPTED");
});
