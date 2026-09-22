#!/usr/bin/env node
import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import { readdirSync, readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const schema = JSON.parse(readFileSync(join(HERE, "invocation.schema.json"), "utf8"));
const ajv = new Ajv2020({ strict: false, allErrors: true });
addFormats(ajv);
ajv.addSchema(schema);
const files = readdirSync(join(HERE, "golden")).filter((name) => name.endsWith(".json"));
let failed = 0;
for (const file of files) {
  const shape = file.split(".")[0];
  const validate = ajv.compile({ $ref: `${schema.$id}#/$defs/${shape}` });
  const value = JSON.parse(readFileSync(join(HERE, "golden", file), "utf8"));
  const expected = !file.includes(".invalid-");
  if (validate(value) === expected) console.log(`  ✓ ${file} ${expected ? `≡ ${shape}` : "is rejected"}`);
  else { console.error(`  ✗ ${file}: ${ajv.errorsText(validate.errors)}`); failed++; }
}
console.log(failed ? `invocation.v2: ${failed} golden(s) FAILED` : `invocation.v2: ${files.length} goldens discriminate`);
process.exit(failed ? 1 : 0);
