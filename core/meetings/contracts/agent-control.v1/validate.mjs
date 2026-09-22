#!/usr/bin/env node
import Ajv2020 from "ajv/dist/2020.js";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const schema = JSON.parse(readFileSync(join(HERE, "agent-control.schema.json"), "utf8"));
const ajv = new Ajv2020({ strict: false, allErrors: true });
ajv.addSchema(schema);
const MAX_ROW_ID = 9223372036854775807n;

function semanticErrors(shape, data) {
  const errors = [];
  if (shape === "DocLinkResponse" && data?.doc) {
    const row = String(data.meeting_id);
    if (data.doc.path !== `kg/entities/meeting/${row}.md`) {
      errors.push("doc.path is not derived from meeting_id");
    }
    if (data.doc.title !== `Meeting ${row}`) {
      errors.push("doc.title is not derived from meeting_id");
    }
  }
  for (const [name, value] of [
    ["meeting_id", data?.meeting_id],
    ["user_id", data?.user_id],
    ["workspace", data?.doc?.workspace],
  ]) {
    if (typeof value === "string" && /^\d+$/.test(value) && BigInt(value) > MAX_ROW_ID) {
      errors.push(`${name} exceeds signed-bigint range`);
    }
  }
  return errors;
}

let failed = 0;
const files = readdirSync(join(HERE, "golden")).filter((name) => name.endsWith(".json"));
for (const file of files) {
  const shape = file.split(".")[0];
  const validate = ajv.compile({ $ref: `${schema.$id}#/$defs/${shape}` });
  const data = JSON.parse(readFileSync(join(HERE, "golden", file), "utf8"));
  const schemaValid = validate(data);
  const semantics = schemaValid ? semanticErrors(shape, data) : [];
  const valid = schemaValid && semantics.length === 0;
  const expected = !file.includes(".invalid-");
  if (valid === expected) {
    console.log(`  ✓ ${file} ${expected ? "conforms" : "is rejected"}`);
  } else {
    const detail = semantics.length ? semantics.join("; ") : ajv.errorsText(validate.errors);
    console.error(`  ✗ ${file} ${expected ? "failed" : "was accepted"}: ${detail}`);
    failed += 1;
  }
}

console.log(failed ? `agent-control.v1: ${failed} golden(s) FAILED` :
  `agent-control.v1: ${files.length} goldens discriminate`);
process.exit(failed ? 1 : 0);
