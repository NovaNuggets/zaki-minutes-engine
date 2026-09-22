# deploy/lite/bin — lite container helper scripts

In-image scripts for the single-container [lite](../README.md) deployment. Copied to
`/usr/local/bin` (or invoked by supervisord) inside `vexa-lite:dev`.

| Script | Role |
|---|---|
| `vexa-bot-launch` | trusted meeting-bot launcher the runtime execs per meeting via the **process backend** (`BOT_COMMAND`). Drops root/capabilities into a distinct per-meeting numeric uid (`vexa-bot` primary group, `pulse-access` supplemental group) before Node starts, then runs against the shared Xvfb/PulseAudio. |
| `vexa-agent-worker` | agent-worker launcher the runtime execs per dispatch (`AGENT_WORKER_COMMAND`) — the claude-in-process turn under the agent venv. |
| `setup-pulseaudio-sinks.sh` | one-shot: builds the `tts_sink → virtual_mic` PulseAudio graph the bot's capture/speak path expects. |
| `provision-key.sh` | background (from the entrypoint): reads the root-only admin credential, mints a self-host API key once admin-api is up, and hands it to the Terminal for zero-login. No-op if `VEXA_API_KEY` is supplied. |
