// release-value-gate — guarantee line 8, enforced (D9/D10). "All release value confirmed accepted."
//
// A release may promote only when EVERY change in the batch was individually proven before it
// entered. The batch = the PRs merged between the previous release tag and this one. Each PR is
// ACCEPTED when its value is machine-witnessed on its OWN merged head (the ladder, not a re-run):
//
//   • runtime PR  → `value-fsm` (the pr-value L3 leg) is GREEN on the PR's head sha
//   • non-runtime → merged through the full `gates` suite (backend-invisible, machine-sound) AND
//                   the diff was accepted at merge: a fresh non-author review approval on the
//                   merged head, or a maintainer author (the merge card's own diff rule)
//
// A RED value-fsm is never rescued by anything else on the PR — a change we cannot positively
// verify fails CLOSED.
//
// D9 RETIRED 2026-09-14 (DEC-2026-09-14-7): `state: value-signed` is no longer a term here. It was
// the July constitution's human sign-off, written before the uniform PR contract (DEC-2026-09-01-2)
// put a non-author reviewer PASS on every PR; it let a docs/CI change into a batch on the label
// alone. The non-runtime leg now re-derives the merge card's diff row instead, so the release-time
// check and the merge-time check ask the same question.
//
// Exit codes (so the published-guard can tell "unwitnessed" from "couldn't evaluate"):
//   0 — every batch change accepted
//   1 — at least one change is DEFINITIVELY unaccepted (retract-worthy)
//   3 — could not evaluate (transient API error, unresolvable ref, or a commit not mapped to a
//       gated PR) — blocks promote, but is NOT a "retract the release" signal
//   2 — usage
//
// Inputs (env): RELEASE_VERSION (vX.Y.Z), GITHUB_REPOSITORY (owner/name). Uses `gh` (GH_TOKEN).

import { execSync } from "node:child_process";
import { pathToFileURL } from "node:url";
// The merge card owns the definition of "diff accepted"; reuse it so release-time and merge-time
// cannot drift apart (importing it is inert — its own main() is guarded on argv[1]).
import { freshApproval, authorIsMaintainer } from "./merge-card-gate.mjs";

const IS_MAIN = import.meta.url === pathToFileURL(process.argv[1] || "").href;
const REPO = process.env.GITHUB_REPOSITORY;
const VERSION = process.env.RELEASE_VERSION;
if (IS_MAIN && (!REPO || !VERSION)) { console.error("release-value-gate: RELEASE_VERSION and GITHUB_REPOSITORY are required"); process.exit(2); }

// pr-value.yml's path filter — the definition of a "runtime surface" (keep in sync with that file).
const RUNTIME_PREFIXES = ["core/", "clients/terminal/", "deploy/compose/", "deploy/lite/", "libs/"];
const RUNTIME_FILES = ["package.json", "pnpm-lock.yaml"];

// Retry gh reads a few times so a transient 5xx/secondary-limit doesn't masquerade as a verdict.
function ghRaw(path) {
  let last;
  for (let i = 0; i < 3; i++) {
    try { return execSync(`gh api "${path}"`, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }); }
    catch (e) { last = e; }
  }
  throw last;
}
const ghj = (path) => JSON.parse(ghRaw(path));

function parseVer(t) {
  const m = String(t).match(/^v?(\d+)\.(\d+)\.(\d+)(?:-(.+))?$/);
  return m ? { core: [+m[1], +m[2], +m[3]], pre: m[4] || null, raw: t } : null;
}
function cmpVer(a, b) {
  for (let i = 0; i < 3; i++) if (a.core[i] !== b.core[i]) return a.core[i] - b.core[i];
  if (a.pre === b.pre) return 0;
  if (!a.pre) return 1; if (!b.pre) return -1;
  return a.pre < b.pre ? -1 : 1;
}

function previousReleaseTag() {
  const cur = parseVer(VERSION);
  if (!cur) throw new Error(`RELEASE_VERSION "${VERSION}" is not vX.Y.Z`);
  const tags = [];
  for (let page = 1; page <= 10; page++) {
    const batch = ghj(`repos/${REPO}/tags?per_page=100&page=${page}`);
    for (const t of batch) { const p = parseVer(t.name); if (p && p.pre === null) tags.push(p); }
    if (batch.length < 100) break;
  }
  const lower = tags.filter((t) => cmpVer(t, cur) < 0).sort(cmpVer);
  return lower.length ? lower[lower.length - 1].raw : null;
}

// Enumerate the range. Returns { prs:Set<number>, unaccounted:[subject], commitCount }.
// A commit is mapped to a PR ONLY by the strict trailing `(#N)` squash form — the loose "first
// #N in the subject" heuristic is dropped (it can grab an issue ref). A commit with no trailing
// (#N) is "unaccounted": we cannot tie it to a gated PR, so it fails closed (exit 3).
function enumerate(prevTag) {
  const range = `${prevTag}...${VERSION}`;
  const prs = new Set(); const unaccounted = []; let commitCount = 0;
  for (let page = 1; page <= 30; page++) {
    const cmp = ghj(`repos/${REPO}/compare/${range}?per_page=100&page=${page}`);
    const commits = cmp.commits || [];
    for (const c of commits) {
      commitCount++;
      const subject = (c.commit?.message || "").split("\n")[0];
      const m = subject.match(/\(#(\d+)\)\s*$/);
      if (m) prs.add(+m[1]);
      else if ((c.parents || []).length < 2) unaccounted.push(`${(c.sha || "").slice(0, 8)} ${subject.slice(0, 70)}`);
      // 2-parent merge commits in a squash-only repo are rare; ignore them, don't fail on them.
    }
    if (commits.length < 100) break;
  }
  return { prs: [...prs].sort((a, b) => a - b), unaccounted, commitCount };
}

function prTouchesRuntime(num) {
  for (let page = 1; page <= 10; page++) {
    const files = ghj(`repos/${REPO}/pulls/${num}/files?per_page=100&page=${page}`);
    for (const f of files) {
      const p = f.filename;
      if (RUNTIME_FILES.includes(p) || RUNTIME_PREFIXES.some((pre) => p.startsWith(pre))) return true;
    }
    if (files.length < 100) break;
  }
  return false;
}

// "success" | "failure" | "absent" — exact value-fsm name only; latest run wins (a re-run to
// green legitimately supersedes), so an auxiliary check whose name merely contains "value" cannot
// mask a failed value-fsm.
function valueFsmVerdict(sha) {
  const runs = (ghj(`repos/${REPO}/commits/${sha}/check-runs?per_page=100`).check_runs || [])
    .filter((r) => r.name === "value-fsm");
  if (!runs.length) return "absent";
  runs.sort((a, b) => new Date(b.started_at || 0) - new Date(a.started_at || 0));
  return runs[0].conclusion === "success" ? "success" : "failure";
}

// The per-PR verdict, pure over the three facts we read about it. Runtime changes stand on
// value-fsm; non-runtime changes stand on the diff acceptance the merge card required at merge
// time (fresh non-author approval, or a maintainer author). Since D9 retired there is no label
// term: a labelless PR that merged under the common bar is ACCEPTED here.
export function batchVerdict({ runtime, vf, diffOk }) {
  if (vf === "success") return { verdict: "ACCEPTED", why: "value-fsm green on head" };
  if (vf === "failure") return { verdict: "UNACCEPTED", why: "value-fsm RED on head — nothing waives it; re-run pr-value green" };
  if (runtime) return { verdict: "UNACCEPTED", why: "runtime PR with no value-fsm run — re-run pr-value on head or (if non-runtime) reclassify" };
  if (diffOk) return { verdict: "ACCEPTED", why: "non-runtime PR; gates-green (merged) + diff accepted at merge" };
  return { verdict: "UNACCEPTED", why: "non-runtime PR with no non-author approval on its merged head — the merge bar was not met" };
}

// ── run ───────────────────────────────────────────────────────────────────────────────────────
function main() {
  let prevTag;
  try { prevTag = previousReleaseTag(); }
  catch (e) { console.error(`::error ::release-value-gate — could not resolve tags: ${e.message}`); process.exit(3); }
  if (!prevTag) { console.error(`::error ::release-value-gate — no prior release tag < ${VERSION}; cannot bound the batch. Resolve manually.`); process.exit(3); }

  let batch;
  try { batch = enumerate(prevTag); }
  catch (e) { console.error(`::error ::release-value-gate — compare ${prevTag}...${VERSION} failed: ${e.message}`); process.exit(3); }

  console.log(`release-value-gate — batch ${prevTag} → ${VERSION}: ${batch.commitCount} commit(s), ${batch.prs.length} PR(s)`);

  if (batch.commitCount === 0) {
    console.error(`::error ::release-value-gate — empty range ${prevTag}...${VERSION}: nothing to release (or a bad tag range). Fails closed.`);
    process.exit(1);
  }

  const rows = [];
  let definitelyUnaccepted = 0;
  let couldNotEvaluate = batch.unaccounted.length; // commits not mappable to a gated PR

  for (const num of batch.prs) {
    let pr;
    try { pr = ghj(`repos/${REPO}/pulls/${num}`); }
    catch { rows.push({ num, verdict: "UNVERIFIABLE", why: "PR fetch failed after retries" }); couldNotEvaluate++; continue; }
    const sha = pr.head?.sha;
    const author = pr.user?.login;
    let runtime, vf, diffOk;
    try {
      runtime = prTouchesRuntime(num);
      vf = sha ? valueFsmVerdict(sha) : "absent";
      // Only the non-runtime leg needs it; do not pay two API calls for a runtime PR value-fsm already settled.
      diffOk = runtime || vf === "success"
        ? true
        : authorIsMaintainer(author) || !!freshApproval(ghj(`repos/${REPO}/pulls/${num}/reviews?per_page=100`), author, sha);
    }
    catch { rows.push({ num, verdict: "UNVERIFIABLE", why: "check-runs/files/reviews fetch failed after retries" }); couldNotEvaluate++; continue; }

    const { verdict, why } = batchVerdict({ runtime, vf, diffOk });
    if (verdict === "UNACCEPTED") definitelyUnaccepted++;
    rows.push({ num, verdict, why });
  }

  console.log("\n| PR | verdict | basis |\n|----|---------|-------|");
  for (const r of rows) console.log(`| #${r.num} | ${r.verdict} | ${r.why} |`);
  if (batch.unaccounted.length) {
    console.log("\nUnaccounted commits (not tied to a gated PR — fail closed):");
    for (const u of batch.unaccounted) console.log(`  · ${u}`);
  }
  console.log("");

  if (definitelyUnaccepted > 0) {
    console.error(`::error ::release-value-gate — ${definitelyUnaccepted} batch change(s) DEFINITIVELY unaccepted (guarantee line 8). Promote blocked.`);
    process.exit(1);
  }
  if (couldNotEvaluate > 0) {
    console.error(`::error ::release-value-gate — ${couldNotEvaluate} change(s) could not be verified (unaccounted commits / API errors). Promote blocked; resolve then re-run.`);
    process.exit(3);
  }
  console.log(`✓ release-value-gate — all ${batch.prs.length} batch PR(s) accepted (guarantee line 8).`);
}

if (IS_MAIN) main();
