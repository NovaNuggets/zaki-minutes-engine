- **zaki-control.v1 gains `GET /{userId}/policy` (#69).** The control plane can now read a subject's
  stored consent policy back verbatim — `capture_enabled`, `agent_read_enabled`,
  `capture_notice_policy_version` and the retention windows — returning `404 policy_not_found` when
  none is stored, with the same token/path/header identity binding as every control route.
  Backward-compatible contract addition; the seal moved one line.
