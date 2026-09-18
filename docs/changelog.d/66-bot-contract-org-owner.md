- **The ZAKI bot Pod contract accepts the bot image under the `novanuggets` GHCR org as well as
  `projectnuggets` (#66).** The `ZAKI_MINUTES_BOT_CONTRACT_JSON` image may now be
  `ghcr.io/novanuggets/zaki-minutes-bot` or `ghcr.io/projectnuggets/zaki-minutes-bot`. The
  `sha-<source sha>` tag and the `sha256` digest are still required, and any other owner or registry
  is still refused at boot, so the bot image can be re-pinned to the org with the same digest
  without the runtime refusing to start. No configuration change for self-hosters. See
  [ZAKI-DOWNSTREAM.md](https://github.com/NovaNuggets/zaki-minutes-engine/blob/main/ZAKI-DOWNSTREAM.md).
