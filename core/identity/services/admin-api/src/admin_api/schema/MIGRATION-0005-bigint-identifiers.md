# MIGRATION-0005 — widen identifiers to signed BIGINT

Minutes' sealed public contracts accept positive signed 64-bit user and meeting identifiers. The
upstream v0.12 schema used PostgreSQL `INTEGER` (`int4`), so accepting the wire contract while leaving
storage at 32 bits would eventually overflow or turn a contract-valid request into a storage error.

`ensure_schema` now performs the only non-add-column type migration in the convergence layer:

- every model-owned identifier and foreign-key column widens from `int2`/`int4` to `BIGINT`;
- each attached SERIAL sequence widens to a bigint sequence;
- the work is one PostgreSQL transaction and is idempotent;
- unexpected source types fail boot instead of being coerced;
- no column, row, constraint, or index is dropped.

The ALTERs take table locks, so run the new image against staging during a maintenance window before
activation and record the duration. An old image remains readable on the expanded schema because
PostgreSQL can return int8 values through SQLAlchemy's integer binding; do not narrow on rollback.
Once any identifier exceeds `2147483647`, narrowing is destructive and forbidden.

Required release evidence:

1. restore a pre-migration snapshot in staging;
2. boot once and verify every listed column plus sequence is bigint;
3. run the signed-bigint boundary and FK tests;
4. roll back to the previous image on the expanded schema and prove normal reads;
5. restore again, rerun the migration, and verify idempotency.
