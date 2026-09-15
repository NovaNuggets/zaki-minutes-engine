- **A meeting's minutes are now written in the language the meeting was held in (#63).** The summary
  prompt always asked for "the meeting's own dominant language", but nothing told the model what that
  language was — a German meeting came back with a German transcript and English minutes. The
  summariser now derives the meeting's language from the transcript segments' own detections, weighted
  by transcribed characters (short interjections in another language cannot outvote the substantive
  discussion), and states it in the prompt. A genuinely mixed meeting keeps the existing
  mixed-language rule. English is deliberately never asserted: the transcriber labels a chunk it could
  not identify as English, so that label is treated as "unknown" rather than as evidence. Prompt input
  only — no output-contract change (the `## TL;DR / ## Key points / ## Decisions` sections are the
  same). Takes effect on the next engine image rebuild + chart repin (owner-gated deploy).
