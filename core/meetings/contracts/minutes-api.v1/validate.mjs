#!/usr/bin/env node
import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const schema = JSON.parse(readFileSync(join(HERE, "minutes-api.schema.json"), "utf8"));
const erasure = JSON.parse(readFileSync(join(HERE, "../erasure.v1/erasure.schema.json"), "utf8"));
const ajv = new Ajv2020({ strict: false, allErrors: true });
addFormats(ajv);
ajv.addSchema(erasure);
ajv.addSchema(schema);
const MAX_ROW_ID = 9223372036854775807n;

function semanticErrors(shape, data) {
  const values = [];
  if (shape === "CaptureResponse") values.push(["id", data?.id]);
  if (shape === "WithdrawalResponse" || shape === "StatusResponse") {
    values.push(["meeting_id", data?.meeting_id]);
  }
  if (shape === "ErasureResponse") {
    values.push(["user_id", data?.subject?.user_id]);
    if (data?.scope === "meeting") values.push(["meeting_id", data?.subject?.meeting_id]);
  }
  return values.flatMap(([name, value]) => (
    typeof value === "string" && /^[1-9][0-9]{0,18}$/.test(value) && BigInt(value) > MAX_ROW_ID
      ? [`${name} exceeds signed-bigint range`]
      : []
  ));
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
    console.error(`  ✗ ${file}: ${expected ? detail : "was accepted"}`);
    failed += 1;
  }
}
console.log(failed ? `minutes-api.v1: ${failed} golden(s) FAILED` : `minutes-api.v1: ${files.length} goldens discriminate`);
process.exit(failed ? 1 : 0);
