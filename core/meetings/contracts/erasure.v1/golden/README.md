# erasure.v1 golden vectors

- The four positive owner/scope variants are intact and fresh: Minutes meeting/account and
  Agent meeting/account. Their subjects use canonical decimal strings.
- The adjacent-large Agent receipt keeps two ids above JavaScript's safe-integer ceiling distinct;
  numeric-subject and signed-bigint-overflow controls are rejected.
- `invalid-wrong-owner` and `invalid-wrong-scope` cross an owner/scope discriminator while
  retaining otherwise well-formed signed fields.
- `invalid-count-subset` and `invalid-count-extra` prove that deletion manifests are exact,
  not open-ended telemetry bags.
- `invalid-tamper` changes a count without recomputing the digest/signature.
- `invalid-replay` is correctly signed but outside the live five-minute acceptance window.

The key and verification clock are public constants in `../validate.mjs`; these are protocol
vectors, never credentials.
