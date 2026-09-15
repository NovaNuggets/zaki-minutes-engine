- **A transcript no longer hides the minutes it lost (#64).** When the speech-to-text provider was
  unavailable, the notetaker retried each chunk, gave up, and dropped the audio — and the transcript
  simply closed over the hole. A meeting with fourteen unavailable minutes read as a complete
  transcript, and its summary read as complete minutes. Lost audio now appears in the transcript
  itself as a speaker-neutral marker naming the span and the reason, e.g.
  `[transcription unavailable 09:03:50–09:17:49 UTC — provider unavailable (HTTP 503)]`. Consecutive
  losses are one marker, not one per retry, and the marker is stored like any other segment, so it
  survives a reload and travels with the transcript wherever it is read or exported.
- **A summary of an incomplete transcript says so.** The summary generator now derives a single line
  from those markers — "N minutes not transcribed (provider unavailable)" — and is told never to
  present the minutes as covering the whole meeting. A meeting whose audio was lost entirely gets no
  summary at all rather than minutes written about its own gaps.
