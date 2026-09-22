# agent-control.v1 goldens

Synthetic, non-personal exact-row responses. The positive examples pin the two HTTP success shapes.
Each `.invalid-` example changes one boundary: unknown fields, native/path identifiers, response-row
mismatch, JSON-number identity, or an out-of-range row/owner. JSON Schema handles shape constraints;
`validate.mjs` enforces the row-derived path/title relationship and signed-bigint string range. The
adjacent-large positive keeps `9007199254740992` and `9007199254740993` distinct as text.
