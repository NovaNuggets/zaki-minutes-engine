#!/usr/bin/env node
import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const schema = JSON.parse(readFileSync(join(HERE, "minutes-finalized.schema.json"), "utf8"));
const ajv = new Ajv2020({ strict: false, allErrors: true });
addFormats(ajv);
ajv.addSchema(schema);

let failed = 0;
const files = readdirSync(join(HERE, "golden")).filter((name) => name.endsWith(".json"));
for (const file of files) {
  const shape = file.split(".")[0];
  const validate = ajv.compile({ $ref: `${schema.$id}#/$defs/${shape}` });
  const value = JSON.parse(readFileSync(join(HERE, "golden", file), "utf8"));
  if (validate(value)) {
    console.log(`  ✓ ${file} ≡ ${shape}`);
  } else {
    console.error(`  ✗ ${file} (${shape}): ${ajv.errorsText(validate.errors)}`);
    failed += 1;
  }
}
console.log(failed ? `minutes-finalized.v1: ${failed} golden(s) FAILED` : `minutes-finalized.v1: ${files.length} goldens conform`);
process.exit(failed ? 1 : 0);
