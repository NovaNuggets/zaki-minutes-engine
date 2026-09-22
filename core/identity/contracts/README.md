# identity/contracts — sealed identity wire shapes

Versioned JSON Schema contracts the identity lane publishes: sealed `identity.v1` plus additive
`identity.v2`, which introduces the negotiated Agent scope without changing v1 bytes. A `.vN` dir
is frozen once deliberately sealed; validate each folder with its `validate.mjs`.

_Governed by `docs/docs/governance/architecture.mdx` (P1–P12). This folder owns one concern; its public surface is its `index`/contract; it may depend only on what the dependency-rules allow._
