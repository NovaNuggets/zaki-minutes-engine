"""Agent consumer tests for the sealed Minutes ``zaki-read.v1`` boundary."""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone

import pytest

from shared.minutes_read import MAX_RESPONSE_BYTES, MinutesReadClient, MinutesReadError


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
READ_TOKEN = "minutes-read-token-0123456789abcdef"


class _Response(io.BytesIO):
    def __init__(self, body: dict, *, status: int = 200, headers: dict | None = None) -> None:
        raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
        super().__init__(raw)
        self.status = status
        self.headers = {"Content-Length": str(len(raw)), **(headers or {})}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class _DeclaredOversize:
    status = 200
    headers = {"Content-Length": str(MAX_RESPONSE_BYTES + 1)}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def read(self, _size):
        raise AssertionError("an oversized declared response body must not be read")


class _RawResponse(io.BytesIO):
    status = 200

    def __init__(self, raw: bytes, *, declare: bool = True) -> None:
        super().__init__(raw)
        self.headers = {"Content-Length": str(len(raw))} if declare else {}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class _ReadFailure:
    status = 200
    headers = {}

    def __init__(self, message: str) -> None:
        self._message = message

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def read(self, _size):
        raise OSError(self._message)


class _LifecycleFailure:
    def __init__(self, stage: str, message: str) -> None:
        self._stage = stage
        self._message = message

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        if self._stage == "close":
            raise OSError(self._message)

    @property
    def headers(self):
        if self._stage == "headers":
            raise OSError(self._message)
        return {}

    @property
    def status(self):
        if self._stage == "status":
            raise OSError(self._message)
        return 200

    def read(self, _size):
        if self._stage == "read":
            raise OSError(self._message)
        return b'{"items":[],"truncated":false}'


@pytest.mark.parametrize(
    "weak_token",
    [
        "too-short",
        " " + READ_TOKEN,
        READ_TOKEN + " ",
        "a" * 16 + "\n" + "b" * 16,
        "a" * 16 + "\x00" + "b" * 16,
        "a" * 16 + "\x7f" + "b" * 16,
        "é" * 32,
        "a" * 513,
    ],
)
def test_client_rejects_read_tokens_the_producer_cannot_compose(weak_token):
    with pytest.raises(ValueError, match="unpadded printable ASCII between 32 and 512"):
        MinutesReadClient("http://meeting-api:8080", weak_token)


@pytest.mark.parametrize("boundary_token", ["a" * 32, "z" * 512])
def test_client_accepts_canonical_read_token_length_boundaries(boundary_token):
    MinutesReadClient("http://meeting-api:8080", boundary_token)


def _metadata(*, meeting_id: int = 41, occurred_at: str = "2026-07-15T09:00:00Z") -> dict:
    return {
        "id": f"transcript:{meeting_id}",
        "kind": "transcript",
        "title": "Launch review transcript",
        "meeting_id": f"meeting:{meeting_id}",
        "occurred_at": occurred_at,
        "updated_at": "2026-07-15T10:01:00Z",
        "sensitivity": "sensitive_pii",
        "retention": {
            "scope": "minutes.transcript",
            "expires_at": "2026-08-15T12:00:00Z",
        },
    }


def _meeting_metadata(*, meeting_id: int = 41, occurred_at: str) -> dict:
    metadata = _metadata(meeting_id=meeting_id, occurred_at=occurred_at)
    metadata.pop("meeting_id")
    metadata["id"] = f"meeting:{meeting_id}"
    metadata["kind"] = "meeting"
    metadata["title"] = "Launch review meeting"
    return metadata


def _item(*, meeting_id: int = 41, occurred_at: str = "2026-07-15T09:00:00Z") -> dict:
    return {
        **_metadata(meeting_id=meeting_id, occurred_at=occurred_at),
        "capture_notice": {
            "bot_visible": True,
            "tenant_attested_at": "2026-07-15T08:55:00Z",
            "policy_version": "minutes-capture.v1",
        },
        "content": {
            "format": "speaker_turns",
            "language": "en",
            "turns": [{
                "speaker": "Participant A",
                "started_at": "2026-07-15T09:00:01Z",
                "ended_at": "2026-07-15T09:00:04Z",
                "text": "We agreed to ship the pilot.",
            }],
        },
    }


def test_last_transcript_reads_index_then_item_with_exact_user_bound_headers():
    requests = []
    responses = iter([
        _Response({"items": [_metadata()], "truncated": False}),
        _Response({"item": _item(), "truncated": False}),
    ])

    def open_request(request, *, timeout):
        requests.append((request, timeout))
        return next(responses)

    turn = MinutesReadClient(
        "https://minutes.internal:8443",
        READ_TOKEN,
        open_fn=open_request,
        now=lambda: NOW,
        request_id=lambda: "request-1",
    ).begin_turn("7")

    result = turn.last_transcript()

    assert result.item["id"] == "transcript:41"
    assert result.summary_fallback is False
    assert [request.full_url for request, _timeout in requests] == [
        "https://minutes.internal:8443/api/zaki/read/v1/7/index?limit=200",
        "https://minutes.internal:8443/api/zaki/read/v1/7/item/transcript%3A41?variant=full",
    ]
    assert all(timeout == 5.0 for _request, timeout in requests)
    for request, _timeout in requests:
        assert request.get_header("X-zaki-read-token") == READ_TOKEN
        assert request.get_header("X-zaki-user-id") == "7"
        assert request.get_header("X-request-id") == "request-1"


def test_item_too_large_retries_once_as_summary_and_marks_answer_only_fallback():
    requests = []
    summary_item = {
        **_item(),
        "content": {"format": "summary", "text": "The team approved the pilot."},
    }
    responses = iter([
        _Response({"items": [_metadata()], "truncated": False}),
        _Response(
            {"error": {"code": "item_too_large", "message": "Item exceeds cap"},
             "truncated": False},
            status=413,
        ),
        _Response({"item": summary_item, "truncated": False}),
    ])

    def open_request(request, *, timeout):
        requests.append(request)
        return next(responses)

    result = MinutesReadClient(
        "http://meeting-api:8080", READ_TOKEN, open_fn=open_request, now=lambda: NOW,
    ).begin_turn(7).last_transcript()

    assert result.summary_fallback is True
    assert result.item["content"] == {
        "format": "summary", "text": "The team approved the pilot.",
    }
    assert [request.full_url.rsplit("?", 1)[-1] for request in requests] == [
        "limit=200", "variant=full", "variant=summary",
    ]


def test_expired_metadata_is_rejected_before_an_item_read():
    expired = {
        **_metadata(),
        "retention": {
            "scope": "minutes.transcript",
            "expires_at": "2026-07-15T11:59:59Z",
        },
    }
    calls = 0

    def open_request(_request, *, timeout):
        nonlocal calls
        calls += 1
        return _Response({"items": [expired], "truncated": False})

    with pytest.raises(MinutesReadError, match="invalid"):
        MinutesReadClient(
            "http://meeting-api:8080", READ_TOKEN, open_fn=open_request, now=lambda: NOW,
        ).begin_turn(7).last_transcript()

    assert calls == 1


def test_schema_valid_out_of_order_transcript_turns_are_rejected_semantically():
    bad_item = _item()
    bad_item["content"]["turns"] = [
        {
            "speaker": "Participant A",
            "started_at": "2026-07-15T09:00:05Z",
            "text": "Second turn in time.",
        },
        {
            "speaker": "Participant B",
            "started_at": "2026-07-15T09:00:02Z",
            "text": "This turn arrived out of order.",
        },
    ]
    responses = iter([
        _Response({"items": [_metadata()], "truncated": False}),
        _Response({"item": bad_item, "truncated": False}),
    ])

    with pytest.raises(MinutesReadError, match="invalid"):
        MinutesReadClient(
            "http://meeting-api:8080", READ_TOKEN,
            open_fn=lambda _request, *, timeout: next(responses), now=lambda: NOW,
        ).begin_turn(7).last_transcript()


def test_item_response_must_match_the_selected_index_identity():
    foreign_item = {
        **_item(),
        "id": "transcript:42",
        "meeting_id": "meeting:42",
    }
    responses = iter([
        _Response({"items": [_metadata()], "truncated": False}),
        _Response({"item": foreign_item, "truncated": False}),
    ])

    with pytest.raises(MinutesReadError, match="identity"):
        MinutesReadClient(
            "http://meeting-api:8080", READ_TOKEN,
            open_fn=lambda _request, *, timeout: next(responses), now=lambda: NOW,
        ).begin_turn(7).last_transcript()


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("updated_at", "2026-07-15T10:02:00Z"),
        (
            "retention",
            {
                "scope": "minutes.transcript",
                "expires_at": "2026-09-15T12:00:00Z",
            },
        ),
    ],
)
def test_item_response_cannot_change_security_metadata_selected_from_index(field, replacement):
    selected = _metadata()
    item = _item()
    item[field] = replacement
    responses = iter([
        _Response({"items": [selected], "truncated": False}),
        _Response({"item": item, "truncated": False}),
    ])

    with pytest.raises(MinutesReadError, match="identity"):
        MinutesReadClient(
            "http://meeting-api:8080", READ_TOKEN,
            open_fn=lambda _request, *, timeout: next(responses), now=lambda: NOW,
        ).begin_turn(7).last_transcript()


def test_declared_oversize_response_is_rejected_before_reading_the_body():
    turn = MinutesReadClient(
        "http://meeting-api:8080", READ_TOKEN,
        open_fn=lambda _request, *, timeout: _DeclaredOversize(), now=lambda: NOW,
    ).begin_turn(7)

    with pytest.raises(MinutesReadError, match="budget"):
        turn.index()

    assert turn.calls == 1
    assert turn.response_bytes == 0


def test_item_content_cap_is_measured_as_compact_utf8_not_character_count():
    oversized = {
        **_item(),
        "content": {"format": "summary", "text": "😀" * 65_535},
    }
    response = _Response({"item": oversized, "truncated": False})
    assert int(response.headers["Content-Length"]) < MAX_RESPONSE_BYTES

    turn = MinutesReadClient(
        "http://meeting-api:8080", READ_TOKEN,
        open_fn=lambda _request, *, timeout: response, now=lambda: NOW,
    ).begin_turn(7)

    with pytest.raises(MinutesReadError, match="content cap"):
        turn.item("transcript:41")


def test_last_transcript_stops_at_first_newest_first_match_without_exhausting_history():
    selected = _metadata(meeting_id=42, occurred_at="2026-07-15T10:00:00Z")
    responses = iter([
        _Response({
            "items": [
                _meeting_metadata(meeting_id=43, occurred_at="2026-07-15T11:00:00Z"),
                selected,
            ],
            "truncated": True,
            "next_cursor": "opaque-page-2-must-not-be-read",
        }),
        _Response({
            "item": _item(meeting_id=42, occurred_at="2026-07-15T10:00:00Z"),
            "truncated": False,
        }),
    ])
    urls = []

    def open_request(request, *, timeout):
        urls.append(request.full_url)
        return next(responses)

    result = MinutesReadClient(
        "http://meeting-api:8080", READ_TOKEN, open_fn=open_request, now=lambda: NOW,
    ).begin_turn(7).last_transcript()

    assert result.item["id"] == "transcript:42"
    assert urls == [
        "http://meeting-api:8080/api/zaki/read/v1/7/index?limit=200",
        "http://meeting-api:8080/api/zaki/read/v1/7/item/transcript%3A42?variant=full",
    ]


def test_last_transcript_fails_closed_when_a_later_page_breaks_newest_first_order():
    responses = iter([
        _Response({
            "items": [
                _meeting_metadata(meeting_id=41, occurred_at="2026-07-15T09:00:00Z"),
            ],
            "truncated": True,
            "next_cursor": "opaque-page-2",
        }),
        _Response({
            "items": [_metadata(meeting_id=42, occurred_at="2026-07-15T10:00:00Z")],
            "truncated": False,
        }),
    ])
    urls = []

    def open_request(request, *, timeout):
        urls.append(request.full_url)
        return next(responses)

    with pytest.raises(MinutesReadError, match="newest-first"):
        MinutesReadClient(
            "http://meeting-api:8080", READ_TOKEN, open_fn=open_request, now=lambda: NOW,
        ).begin_turn(7).last_transcript()

    assert len(urls) == 2
    assert urls[1].endswith("?limit=200&cursor=opaque-page-2")


def test_last_transcript_fails_closed_on_disorder_within_the_selected_page():
    response = _Response({
        "items": [
            _metadata(meeting_id=41, occurred_at="2026-07-15T10:00:00Z"),
            _meeting_metadata(meeting_id=42, occurred_at="2026-07-15T11:00:00Z"),
        ],
        "truncated": False,
    })
    calls = 0

    def open_request(_request, *, timeout):
        nonlocal calls
        calls += 1
        return response

    with pytest.raises(MinutesReadError, match="newest-first"):
        MinutesReadClient(
            "http://meeting-api:8080", READ_TOKEN, open_fn=open_request, now=lambda: NOW,
        ).begin_turn(7).last_transcript()

    assert calls == 1


def test_413_fallback_requires_an_actual_summary_content_variant():
    responses = iter([
        _Response({"items": [_metadata()], "truncated": False}),
        _Response({
            "error": {"code": "item_too_large", "message": "Item exceeds cap"},
            "truncated": False,
        }, status=413),
        _Response({"item": _item(), "truncated": False}),
    ])

    with pytest.raises(MinutesReadError, match="summary variant"):
        MinutesReadClient(
            "http://meeting-api:8080", READ_TOKEN,
            open_fn=lambda _request, *, timeout: next(responses), now=lambda: NOW,
        ).begin_turn(7).last_transcript()


def test_every_transport_attempt_counts_and_ninth_call_is_blocked_before_network():
    attempts = 0

    def unavailable(_request, *, timeout):
        nonlocal attempts
        attempts += 1
        raise OSError("unavailable")

    turn = MinutesReadClient(
        "http://meeting-api:8080", READ_TOKEN, open_fn=unavailable, now=lambda: NOW,
    ).begin_turn(7)
    for _ in range(8):
        with pytest.raises(MinutesReadError, match="transport"):
            turn.index()

    with pytest.raises(MinutesReadError, match="call budget"):
        turn.index()
    assert turn.calls == 8
    assert attempts == 8


def test_transport_failure_does_not_retain_a_credential_bearing_exception():
    secret = READ_TOKEN

    def leaked_request(_request, *, timeout):
        raise OSError(f"failed request carried X-Zaki-Read-Token: {secret}")

    turn = MinutesReadClient(
        "http://meeting-api:8080", secret, open_fn=leaked_request, now=lambda: NOW,
    ).begin_turn(7)

    with pytest.raises(MinutesReadError, match="transport") as raised:
        turn.index()

    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


def test_request_construction_failure_is_sanitized_without_exception_chaining():
    secret = READ_TOKEN

    def sensitive_request_id():
        raise OSError(f"request setup retained X-Zaki-Read-Token: {secret}")

    turn = MinutesReadClient(
        "http://meeting-api:8080",
        secret,
        open_fn=lambda *_args, **_kwargs: pytest.fail("transport must not be reached"),
        now=lambda: NOW,
        request_id=sensitive_request_id,
    ).begin_turn(7)

    with pytest.raises(MinutesReadError, match="transport") as raised:
        turn.index()

    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


def test_prewrapped_public_read_error_from_transport_cannot_bypass_sanitization():
    secret = READ_TOKEN

    def sensitive_request_id():
        raise MinutesReadError(f"request setup retained X-Zaki-Read-Token: {secret}")

    turn = MinutesReadClient(
        "http://meeting-api:8080",
        secret,
        open_fn=lambda *_args, **_kwargs: pytest.fail("transport must not be reached"),
        now=lambda: NOW,
        request_id=sensitive_request_id,
    ).begin_turn(7)

    with pytest.raises(MinutesReadError, match="transport") as raised:
        turn.index()

    assert str(raised.value) == "Minutes read transport failed"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_stream_failure_does_not_retain_a_credential_bearing_exception():
    secret = READ_TOKEN
    turn = MinutesReadClient(
        "http://meeting-api:8080",
        secret,
        open_fn=lambda _request, *, timeout: _ReadFailure(
            f"failed stream carried X-Zaki-Read-Token: {secret}",
        ),
        now=lambda: NOW,
    ).begin_turn(7)

    with pytest.raises(MinutesReadError, match="transport") as raised:
        turn.index()

    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


@pytest.mark.parametrize("stage", ["headers", "read", "status", "close"])
def test_entire_response_lifecycle_is_sanitized_without_exception_chaining(stage):
    secret = READ_TOKEN
    turn = MinutesReadClient(
        "http://meeting-api:8080",
        secret,
        open_fn=lambda _request, *, timeout: _LifecycleFailure(
            stage,
            f"{stage} retained X-Zaki-Read-Token: {secret}",
        ),
        now=lambda: NOW,
    ).begin_turn(7)

    with pytest.raises(MinutesReadError, match="transport") as raised:
        turn.index()

    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None


def test_response_bodies_share_one_mib_turn_budget_across_calls():
    base = json.dumps({"items": [], "truncated": False}, separators=(",", ":")).encode()
    raw = base + b" " * (220_000 - len(base))
    turn = MinutesReadClient(
        "http://meeting-api:8080", READ_TOKEN,
        open_fn=lambda _request, *, timeout: _RawResponse(raw), now=lambda: NOW,
    ).begin_turn(7)

    for _ in range(4):
        assert turn.index() == {"items": [], "truncated": False}
    with pytest.raises(MinutesReadError, match="response budget"):
        turn.index()

    assert turn.response_bytes == 880_000


def test_chunked_response_without_length_is_stopped_at_the_streamed_cap():
    base = json.dumps({"items": [], "truncated": False}, separators=(",", ":")).encode()
    raw = base + b" " * (MAX_RESPONSE_BYTES + 1 - len(base))
    turn = MinutesReadClient(
        "http://meeting-api:8080", READ_TOKEN,
        open_fn=lambda _request, *, timeout: _RawResponse(raw, declare=False), now=lambda: NOW,
    ).begin_turn(7)

    with pytest.raises(MinutesReadError, match="response budget"):
        turn.index()

    assert turn.response_bytes == MAX_RESPONSE_BYTES + 1
