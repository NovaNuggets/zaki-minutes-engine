from datetime import datetime, timezone

import pytest

from meeting_api.meeting_writes import (
    MAX_FINAL_TRANSCRIPT_SEGMENTS,
    meeting_write_lock_key,
    validated_transcript_finalization_marker,
)


@pytest.mark.parametrize("meeting_id", [1, 2_147_483_648, 9_223_372_036_854_775_807])
def test_meeting_write_lock_key_covers_the_positive_signed_bigint_domain(meeting_id):
    assert meeting_write_lock_key(meeting_id) == -meeting_id


@pytest.mark.parametrize(
    "meeting_id",
    [True, 0, -1, 9_223_372_036_854_775_808, "1", None],
)
def test_meeting_write_lock_key_rejects_values_outside_the_meeting_id_domain(meeting_id):
    with pytest.raises(ValueError, match="meeting id"):
        meeting_write_lock_key(meeting_id)


def test_finalization_marker_rejects_segment_counts_above_the_processing_bound():
    marker = {
        "state": "finalized",
        "revision": "sha256:" + "a" * 64,
        "finalized_at": datetime.now(timezone.utc).isoformat(),
        "segment_count": MAX_FINAL_TRANSCRIPT_SEGMENTS + 1,
    }

    assert validated_transcript_finalization_marker({
        "zaki_transcript_finalization": marker,
    }) is None
