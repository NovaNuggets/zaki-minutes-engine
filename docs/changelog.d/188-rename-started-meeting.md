- **Untitled captures get a real name, and started meetings can be renamed (L-0188).**
  A capture the user never titled no longer lists as "Meeting \<row id\>" — the read plane's
  `_title()` now falls back to the platform label plus the UTC start instant (e.g.
  "Google Meet · 2026-07-16 09:00 UTC"), which owners recognize and which never exposes the
  native meeting id. And `PATCH /meetings/{id}` now accepts a **title-only** patch on a row
  the bot FSM already owns — a rename is a label, not lifecycle state — while every other
  field of a started meeting still returns 409, including a patch that mixes title with
  another key. A rename on a live (non-terminal) row writes only `data.title` — it does NOT
  move `updated_at`, the FSM's staleness clock, so renaming a stuck `stopping`/`active`
  meeting can no longer delay the stop backstop or the reconcile reap by a window.
