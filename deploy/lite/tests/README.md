# deploy/lite/tests — smoke tests against the PUBLISHED lite image

- `concurrent-bots.sh` — the release smoke test and the **sole issuer** of the
  `release/vm-validated` commit status: ≥2 concurrent bots must reach `joining`
  on per-bot profile dirs with zero Chromium SingletonLock signatures (the #478
  failure class fires at browser launch, so no meeting admission is needed).
  Runs in CI as a `release-images / validate-lite` step against the published image, and
  on any clean host after `IMAGE_TAG=vX.Y.Z make lite`; post the attestation with
  `POST_STATUS=1 GIT_SHA=<released sha>` (sole issuer of `release/vm-validated`).
- `local-bind-contract.sh` — static proof that Lite's privileged local identity stays on a real
  loopback publication.
- `minutes-default-off-contract.sh` — static plus executable startup proof that Lite refuses every
Minutes setting (including false flags and historical erasure keys), preserves ordinary upstream
bot defaults, and keeps Minutes material out of supervised services. It also checks trust-domain
configuration projection (including Agent-only environment projection of the optional previous
Gateway verifier, without claiming peer-root isolation),
bounded no-eviction Redis, and startup-log redaction for credentialed Redis URLs.
- `bot-privilege-contract.sh` — static release proof that the Lite image and trusted launcher drop
  meeting bots into the dedicated capability-free `vexa-bot` identity before Node starts.
- `bot-privilege-permission-test.sh` — executable Linux permission proof (using a small Node base,
  not the full Lite build): app/audio/`/tmp` stay usable while control-plane secret files below
  `/run/vexa` remain inaccessible to both a bot and a representative agent uid.
- `secret-projection-permission-test.sh` — executable non-Minutes entrypoint success-path proof that
  normal bots remain available, secret files are root-owned `0600`, and credential values are absent
  from the supervisor parent environment. Lite is explicitly one trusted OS process domain.
