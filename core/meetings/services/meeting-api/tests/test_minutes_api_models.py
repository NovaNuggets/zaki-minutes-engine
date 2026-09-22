"""Exact Python serializers for the sealed minutes-api.v1 JSON boundary."""
from __future__ import annotations

from pydantic import ValidationError
import pytest

from meeting_api.minutes_api_models import (
    MinutesCaptureResponse,
    MinutesStatusResponse,
)


def test_adjacent_ids_above_javascript_safe_integer_roundtrip_as_distinct_strings():
    values = ("9007199254740992", "9007199254740993")

    encoded = [
        MinutesCaptureResponse.model_validate({
            "id": value,
            "status": "requested",
        }).model_dump(mode="json")["id"]
        for value in values
    ]

    assert encoded == list(values)
    assert encoded[0] != encoded[1]


@pytest.mark.parametrize("value", [9007199254740993, True, "01", "9223372036854775808"])
def test_public_models_reject_numeric_ambiguous_or_out_of_range_ids(value):
    with pytest.raises(ValidationError):
        MinutesCaptureResponse.model_validate({"id": value, "status": "requested"})


@pytest.mark.parametrize(
    "payload",
    [
        {"meeting_id": "41", "status": "active", "completion_reason": "stopped"},
        {
            "meeting_id": "41",
            "status": "completed",
            "completion_reason": "stopped",
            "failure_stage": "active",
        },
        {
            "meeting_id": "41",
            "status": "failed",
            "failure_stage": "active",
            "completion_reason": "stopped",
        },
    ],
)
def test_status_discriminator_forbids_terminal_attribution_on_the_wrong_branch(payload):
    with pytest.raises(ValidationError):
        MinutesStatusResponse.model_validate(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"meeting_id": "41", "status": "active"},
        {"meeting_id": "41", "status": "completed", "completion_reason": "stopped"},
        {"meeting_id": "41", "status": "failed", "failure_stage": "active"},
    ],
)
def test_status_discriminator_serializes_each_exact_branch(payload):
    response = MinutesStatusResponse.model_validate(payload)

    assert response.model_dump(mode="json") == payload
