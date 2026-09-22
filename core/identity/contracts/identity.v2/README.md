# identity.v2 — Agent-scoped identity

`identity.v2` is the additive successor to the sealed `identity.v1` contract. It preserves the
existing scoped-token, dispatch-claims, and access-decision shapes and adds the dedicated `agent`
scope. `subject` remains a canonical decimal/string principal on JSON boundaries; it is never a
JSON number.

Rollout is capability-routed. Admin API clients first read `GET /admin/capabilities`. They may ask
for `contract_version=identity.v2` and use `agent` only when that response explicitly advertises
`identity.v2`. A missing capability route means an old v1 server. Once v2 is advertised, a failed
v2 mint is an error—clients must not silently retry with broader legacy credentials or a v1 token.

Goldens use the `<Shape>.<case>.json` convention and are validated by `validate.mjs`.
