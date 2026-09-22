# MIGRATION-0004 — durable Minutes erasure receipts

`minutes_erasure_receipts` is an additive, content-free table keyed by `(user_id, meeting_id)`.
It is written in the same PostgreSQL transaction that deletes the owned meeting row, so a client
retry after a lost response receives the original counts without reintroducing meeting content.

The existing additive `ensure_schema()` pass creates the table before Minutes erasure is enabled.
Production rollout must verify the table and `ix_minutes_erasure_receipts_user` exist before setting
`ZAKI_MINUTES_CAPTURE_ENABLED=true`. The row contains only numeric identifiers, policy/time evidence,
and deletion counts; transcripts, summaries, native meeting identifiers, URLs, and storage keys are
forbidden.
