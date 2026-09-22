#!/usr/bin/env node
import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const schema = JSON.parse(readFileSync(join(HERE, "erasure.schema.json"), "utf8"));
const ajv = new Ajv2020({ strict: false, allErrors: true });
addFormats(ajv);
ajv.addSchema(schema);
const validate = ajv.compile({ $ref: `${schema.$id}#/$defs/SignedReceipt` });

const SECRET = "erasure-golden-secret-v1";
const NOW_MS = Date.parse("2026-07-15T12:30:00Z");
const MAX_AGE_MS = 5 * 60 * 1000;
const MAX_ROW_ID = 9223372036854775807n;

function ordered(value) {
  if (Array.isArray(value)) return value.map(ordered);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, ordered(value[key])]));
  }
  return value;
}

function canonical(value) {
  return Buffer.from(JSON.stringify(ordered(value)), "utf8");
}

function equalText(left, right) {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}

function verifies(receipt) {
  if (!validate(receipt)) return false;
  for (const value of [receipt.subject.user_id, receipt.subject.meeting_id]) {
    if (value !== undefined && BigInt(value) > MAX_ROW_ID) return false;
  }
  const { digest: storedDigest, signature: storedSignature, ...base } = receipt;
  const digest = `sha256=${createHash("sha256").update(canonical(base)).digest("hex")}`;
  if (!equalText(storedDigest, digest)) return false;
  const signature = `sha256=${createHmac("sha256", SECRET)
    .update(canonical({ ...base, digest }))
    .digest("hex")}`;
  if (!equalText(storedSignature, signature)) return false;
  const issued = Date.parse(receipt.issued_at);
  return Number.isFinite(issued) && issued <= NOW_MS + 30_000 && NOW_MS - issued <= MAX_AGE_MS;
}

let failed = 0;
const files = readdirSync(join(HERE, "golden")).filter((name) => name.endsWith(".json"));
for (const file of files) {
  const receipt = JSON.parse(readFileSync(join(HERE, "golden", file), "utf8"));
  const expected = !file.includes(".invalid-");
  const actual = verifies(receipt);
  if (actual === expected) {
    console.log(`  ✓ ${file} ${expected ? "verifies" : "is rejected"}`);
  } else {
    console.error(`  ✗ ${file} ${expected ? "failed verification" : "was accepted"}`);
    failed += 1;
  }
}
console.log(failed ? `erasure.v1: ${failed} golden(s) FAILED` : `erasure.v1: ${files.length} goldens discriminate`);
process.exit(failed ? 1 : 0);
