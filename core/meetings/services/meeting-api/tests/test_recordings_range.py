"""recordings — HTTP Range on the raw master byte route (recordings P3, BUG R1).

The dashboard <audio>/<video> element + the proxy SEND/forward ``Range``; the source must honor it
so seeking fetches only a byte window instead of the whole master. Drives the SHIPPED ``build_router``
over the in-memory fakes, OFFLINE (no MinIO, no DB): a finalized media-file with known master bytes
in ``InMemoryStorage``, then asserts the 200/206/416 contract on
``GET /recordings/{id}/media/{media_file_id}/raw``.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from meeting_api.recordings import build_router
from meeting_api.recordings.fakes import InMemoryRecordingRepo, InMemoryStorage
from meeting_api.recordings.router import (
    DEFAULT_RECORDING_FINALIZE_CONCURRENCY,
    DEFAULT_RECORDING_MEMORY_BUDGET_BYTES,
    DEFAULT_RECORDING_RANGE_MAX_BYTES,
)
from meeting_api.recordings.service import DEFAULT_RECORDING_MAX_TOTAL_BYTES

USER = 7
MEETING_ID = 1
RECORDING_ID = 100
MEDIA_FILE_ID = 11
STORAGE_PATH = "recordings/7/100/conn-abc/audio/master.wav"
# Distinct, position-checkable master bytes so a slice can be verified exactly.
MASTER = bytes(range(256))  # 256 bytes: byte i == i


def _client():
    """A seeded client: one finalized audio media-file whose master bytes live in storage."""
    repo = InMemoryRecordingRepo()
    storage = InMemoryStorage()
    storage.blobs[STORAGE_PATH] = MASTER
    storage.content_types[STORAGE_PATH] = "audio/wav"
    repo.seed(meeting_id=MEETING_ID, user_id=USER, session_uid="conn-abc")
    repo._meetings[MEETING_ID]["recordings"] = [
        {
            "id": RECORDING_ID,
            "session_uid": "conn-abc",
            "source": "bot",
            "status": "completed",
            "media_files": [
                {
                    "id": MEDIA_FILE_ID,
                    "type": "audio",
                    "format": "wav",
                    "is_final": True,
                    "finalized_by": "recording_finalizer.master",
                    "storage_path": STORAGE_PATH,
                }
            ],
        }
    ]
    app = FastAPI()
    app.include_router(build_router(repo, storage))
    return TestClient(app)


_URL = f"/recordings/{RECORDING_ID}/media/{MEDIA_FILE_ID}/raw?type=audio"
_HDRS = {"x-user-id": str(USER)}


def test_no_range_returns_full_body_with_accept_ranges():
    client = _client()
    r = client.get(_URL, headers=_HDRS)
    assert r.status_code == 200, r.text
    assert r.headers["accept-ranges"] == "bytes"
    assert r.headers["content-length"] == str(len(MASTER))
    assert r.content == MASTER


def test_prefix_range_returns_206_first_ten_bytes():
    client = _client()
    r = client.get(_URL, headers={**_HDRS, "Range": "bytes=0-9"})
    assert r.status_code == 206, r.text
    assert r.headers["content-range"] == f"bytes 0-9/{len(MASTER)}"
    assert r.headers["accept-ranges"] == "bytes"
    assert r.headers["content-length"] == "10"
    assert r.content == MASTER[0:10]
    assert len(r.content) == 10


def test_mid_file_range_returns_correct_slice():
    client = _client()
    r = client.get(_URL, headers={**_HDRS, "Range": "bytes=100-149"})
    assert r.status_code == 206, r.text
    assert r.headers["content-range"] == f"bytes 100-149/{len(MASTER)}"
    assert r.headers["content-length"] == "50"
    assert r.content == MASTER[100:150]


def test_open_ended_range_runs_to_eof():
    client = _client()
    total = len(MASTER)
    r = client.get(_URL, headers={**_HDRS, "Range": "bytes=250-"})
    assert r.status_code == 206, r.text
    assert r.headers["content-range"] == f"bytes 250-{total - 1}/{total}"
    assert r.content == MASTER[250:]


def test_suffix_range_returns_last_n_bytes():
    client = _client()
    total = len(MASTER)
    r = client.get(_URL, headers={**_HDRS, "Range": "bytes=-16"})
    assert r.status_code == 206, r.text
    assert r.headers["content-range"] == f"bytes {total - 16}-{total - 1}/{total}"
    assert r.content == MASTER[-16:]


def test_unsatisfiable_range_returns_416():
    client = _client()
    total = len(MASTER)
    r = client.get(_URL, headers={**_HDRS, "Range": f"bytes={total}-"})
    assert r.status_code == 416, r.text
    assert r.headers["content-range"] == f"bytes */{total}"


def test_end_past_eof_is_clamped():
    client = _client()
    total = len(MASTER)
    r = client.get(_URL, headers={**_HDRS, "Range": "bytes=200-99999"})
    assert r.status_code == 206, r.text
    assert r.headers["content-range"] == f"bytes 200-{total - 1}/{total}"
    assert r.content == MASTER[200:]


def test_over_limit_master_is_rejected_before_fetching_its_body(monkeypatch):
    body_reads = 0
    original_get = InMemoryStorage.get

    async def tracking_get(self, key):
        nonlocal body_reads
        body_reads += 1
        return await original_get(self, key)

    monkeypatch.setenv("RECORDING_MAX_TOTAL_BYTES", "128")
    monkeypatch.setattr(InMemoryStorage, "get", tracking_get)
    client = _client()

    response = client.get(_URL, headers=_HDRS)

    assert response.status_code == 413
    assert response.json() == {"detail": "Recording exceeds configured limits"}
    assert body_reads == 0


def test_default_recording_budget_fits_below_the_one_gibibyte_pod_limit():
    # Finalization currently holds the source chunks plus the assembled master and may transiently
    # hand a third representation to the object-store adapter. Keep the configured concurrent
    # working sets inside the explicit process budget rather than relying on the pod OOM killer.
    estimated_finalize_bytes = (
        DEFAULT_RECORDING_MAX_TOTAL_BYTES * 3 * DEFAULT_RECORDING_FINALIZE_CONCURRENCY
    )
    assert DEFAULT_RECORDING_MAX_TOTAL_BYTES <= 64 * 1024 * 1024
    assert DEFAULT_RECORDING_RANGE_MAX_BYTES <= 8 * 1024 * 1024
    assert estimated_finalize_bytes < DEFAULT_RECORDING_MEMORY_BUDGET_BYTES
    assert DEFAULT_RECORDING_MEMORY_BUDGET_BYTES < 1024 * 1024 * 1024


def test_range_larger_than_the_bounded_window_is_rejected_before_storage_read(monkeypatch):
    body_reads = 0
    range_reads = 0
    original_get = InMemoryStorage.get
    original_get_range = InMemoryStorage.get_range

    async def tracking_get(self, key):
        nonlocal body_reads
        body_reads += 1
        return await original_get(self, key)

    async def tracking_get_range(self, key, start, end):
        nonlocal range_reads
        range_reads += 1
        return await original_get_range(self, key, start, end)

    monkeypatch.setenv("RECORDING_RANGE_MAX_BYTES", "32")
    monkeypatch.setattr(InMemoryStorage, "get", tracking_get)
    monkeypatch.setattr(InMemoryStorage, "get_range", tracking_get_range)
    client = _client()

    response = client.get(_URL, headers={**_HDRS, "Range": "bytes=0-63"})

    assert response.status_code == 413
    assert response.json() == {"detail": "Requested recording range exceeds the window limit"}
    assert body_reads == 0
    assert range_reads == 0


def test_unsafe_recording_memory_configuration_fails_at_router_build(monkeypatch):
    monkeypatch.setenv("RECORDING_MAX_TOTAL_BYTES", str(512 * 1024 * 1024))
    monkeypatch.setenv("RECORDING_FINALIZE_CONCURRENCY", "2")
    monkeypatch.setenv("RECORDING_MEMORY_BUDGET_BYTES", str(512 * 1024 * 1024))

    repo = InMemoryRecordingRepo()
    storage = InMemoryStorage()
    try:
        build_router(repo, storage)
    except RuntimeError as error:
        assert "recording memory budget" in str(error)
    else:
        raise AssertionError("unsafe recording memory configuration was accepted")
