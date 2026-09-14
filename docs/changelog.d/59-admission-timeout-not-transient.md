- **A notetaker that nobody lets in is no longer classed as a retryable fault (#59).** The engine's
  join-retry policy treated "the notetaker waited out the waiting room and nobody clicked Admit" the
  same way as a join that failed on our side, and would have sent up to three notetakers in a row to
  wait the full waiting-room window each for the same host. Nobody clicking Admit is a decision, not
  a fault that clears on its own, so the policy now classes it as final: one notetaker, one wait, one
  answer. The reason recorded against the capture is unchanged. No live behaviour changes with this
  release — the automatic re-spawn this policy governs is not wired into the running control plane
  today; the policy is corrected before it is.
