# agent-control.v1 — Agent ↔ Meetings exact-row control

Meetings owns this narrow internal HTTP contract. The Agent control plane uses it to resolve the
immutable owner of one numeric meeting row and, after post-processing, ask Meetings to attach the
canonical generated document to that same row. Agent sends neither a user/native identity carrier
nor a document body; Meetings derives every returned identity-bearing field from its locked row.

## HTTP profile

```text
GET  /internal/meetings/{meeting_id}/owner → OwnerResponse
POST /internal/meetings/{meeting_id}/docs  → DocLinkResponse
```

Both routes require the deployment-owned `X-Internal-Secret`, refuse redirects, and expose no
meeting content. Missing or malformed credentials return `403`; an unavailable configured authority
returns `503`; an absent row returns `404`. The document operation also uses `404` when retention or
erasure makes the exact row non-writable. Success responses must be bounded and non-cacheable.

## Canonical row identity

`meeting_id`, `user_id`, and `workspace` are canonical positive decimal **strings** on JSON. JSON
numbers are rejected even when their current value appears safe: keeping the representation textual
prevents adjacent PostgreSQL bigint rows from collapsing in JavaScript. Producers explicitly convert
their internal Python/SQL integers at serialization; consumers range-check the text before any
conversion back to an internal integer.

`meeting_id` is the only cross-process meeting identifier. The exact responses are:

```json
{ "meeting_id": "42", "user_id": "7" }
```

```json
{
  "meeting_id": "42",
  "doc": {
    "workspace": "7",
    "path": "kg/entities/meeting/42.md",
    "title": "Meeting 42",
    "kind": "meeting"
  }
}
```

The doc
link is deterministic:

```text
workspace = decimal database owner id
path      = kg/entities/meeting/{meeting_id}.md
title     = Meeting {meeting_id}
kind      = meeting
```

Native platform meeting codes never appear in this contract. They can repeat across tenants and may
contain path or prompt metacharacters, so they are unsuitable as storage identities. The validator
adds semantic checks tying `path` and `title` to the response's exact `meeting_id` and bounding
decimal strings to PostgreSQL signed-bigint range. The executable vectors include adjacent values
above JavaScript's safe-integer ceiling plus negative JSON-number and signed-bigint-overflow controls.

Goldens under `golden/` are the executable producer/consumer profile. Files containing `.invalid-`
are negative controls and must be rejected by `validate.mjs`.
