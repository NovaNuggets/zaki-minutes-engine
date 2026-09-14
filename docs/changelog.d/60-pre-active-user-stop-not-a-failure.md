- **Cancelling a notetaker before it is let in is no longer recorded as a failed capture (#60).** When
  you stopped a notetaker while it was still waiting to be admitted, the meeting was recorded as
  `failed` — the same outcome as a notetaker nobody let in or one that crashed — so a deliberate
  cancellation counted as a capture failure in every failure count built on the meeting record. Such
  a meeting now ends `completed` with the reason `stopped`, and it keeps the stage the notetaker had
  reached (`failure_stage`: `joining` or `awaiting_admission`), so it is still told apart from a
  meeting that was actually captured. A notetaker that is shut down without you asking is still
  recorded as `failed`. Nothing changes in what the ZAKI Hub is told or shows: its contract has no
  non-failure outcome for a notetaker that was never admitted, so a cancelled waiting-room stay still
  reaches it as `failed` / `join_denied` until that contract gains one.
