"""MeetingToken-bound HTTP ingress for managed invocation.v2 bots.

Managed bots are ephemeral, browser-facing workloads and must never receive the control-plane Redis
credential.  This router is the complete-mediation boundary: it derives the meeting from the signed
token plus the authoritative session row, validates one bounded transcript.v1 segment, and then
drives the existing collector ingest/retention lease with server-owned adapters.
"""
from __future__ import annotations

import hmac
import json
from functools import lru_cache
from pathlib import Path
from typing import Optional

import jsonschema
from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse
from referencing import Registry, Resource

from ..bot_spawn.invocation import verify_meeting_token
from .ingest import _coerce_segment, ingest


MAX_MANAGED_TRANSCRIPT_BODY_BYTES = 64 * 1024
_INGEST_FIELDS = frozenset({"connection_id", "segment"})
_FENCE_FIELDS = frozenset({"connection_id", "raw", "processed"})
_NO_STORE = {"Cache-Control": "no-store", "Pragma": "no-cache"}


def _json(body: dict, status_code: int) -> JSONResponse:
    return JSONResponse(content=body, status_code=status_code, headers=_NO_STORE)


async def _body(request: Request) -> dict:
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            length = int(declared)
        except ValueError:
            raise ValueError("invalid request length") from None
        if length < 0:
            raise ValueError("invalid request length")
        if length > MAX_MANAGED_TRANSCRIPT_BODY_BYTES:
            raise OverflowError("request body is too large")
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_MANAGED_TRANSCRIPT_BODY_BYTES:
            raise OverflowError("request body is too large")
        chunks.append(chunk)
    try:
        value = json.loads(b"".join(chunks))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("invalid JSON body") from None
    if not isinstance(value, dict):
        raise ValueError("body must be an object")
    return value


def _bearer(authorization: Optional[str]) -> Optional[str]:
    if not isinstance(authorization, str):
        return None
    scheme, separator, token = authorization.partition(" ")
    if separator != " " or scheme.lower() != "bearer" or not token or " " in token:
        return None
    return token


@lru_cache(maxsize=1)
def _segment_validator() -> jsonschema.Draft202012Validator:
    relative = Path("meetings") / "contracts" / "transcript.v1" / "transcript.schema.json"
    for parent in Path(__file__).resolve().parents:
        candidate = parent / relative
        if candidate.is_file():
            schema = json.loads(candidate.read_text(encoding="utf-8"))
            return jsonschema.Draft202012Validator(
                {"$ref": f"{schema['$id']}#/$defs/TranscriptSegment"},
                registry=Registry().with_resource(
                    schema["$id"], Resource.from_contents(schema)
                ),
                format_checker=jsonschema.FormatChecker(),
            )
    raise RuntimeError("transcript.v1 schema is unavailable")


def _segment_conforms(value: object) -> bool:
    try:
        _segment_validator().validate(value)
    except (jsonschema.ValidationError, jsonschema.SchemaError):
        return False
    return isinstance(value, dict) and _coerce_segment(value) is not None


def _verify_token(
    *,
    authorization: Optional[str],
    token_secret: Optional[str],
) -> tuple[Optional[dict], Optional[JSONResponse]]:
    bearer = _bearer(authorization)
    if bearer is None:
        return None, _json({"detail": "missing transcript ingress token"}, 401)
    try:
        claims = verify_meeting_token(bearer, purpose="transcript", secret=token_secret)
    except ValueError:
        return None, _json({"detail": "invalid transcript ingress token"}, 401)
    return claims, None


async def _bind_session(
    *,
    claims: dict,
    connection_id: object,
    meeting_repo,
) -> Optional[JSONResponse]:
    if (
        not isinstance(connection_id, str)
        or not connection_id
        or len(connection_id) > 256
        or not hmac.compare_digest(claims["session_uid"], connection_id)
    ):
        return _json({"detail": "forbidden"}, 403)
    try:
        authoritative = await meeting_repo.get_meeting_id_by_session(session_uid=connection_id)
    except Exception:
        return _json({"detail": "transcript authorization unavailable; retry"}, 503)
    if authoritative != claims["meeting_id"]:
        return _json({"detail": "forbidden"}, 403)
    return None


def build_bot_ingress_router(
    *,
    store,
    redis,
    meeting_repo,
    token_secret: Optional[str],
    carrier_fencer=None,
) -> APIRouter:
    router = APIRouter()

    @router.post("/bots/internal/transcripts/ingest", include_in_schema=False)
    async def ingest_managed_transcript(
        request: Request,
        authorization: Optional[str] = Header(default=None),
    ):
        if token_secret is None:
            return _json({"detail": "transcript ingress unavailable"}, 503)
        claims, denial = _verify_token(
            authorization=authorization,
            token_secret=token_secret,
        )
        if denial is not None:
            return denial
        assert claims is not None
        try:
            body = await _body(request)
        except OverflowError:
            return _json({"detail": "request body is too large"}, 413)
        except ValueError as error:
            return _json({"detail": str(error)}, 400)
        if set(body) != _INGEST_FIELDS or not _segment_conforms(body.get("segment")):
            return _json({"detail": "off-contract transcript ingress"}, 422)
        denial = await _bind_session(
            claims=claims,
            connection_id=body.get("connection_id"),
            meeting_repo=meeting_repo,
        )
        if denial is not None:
            return denial
        if redis is None:
            return _json({"detail": "transcript ingress unavailable; retry"}, 503)
        payload = {
            "type": "transcription",
            "meeting_id": claims["meeting_id"],
            "native_meeting_id": claims.get("native_meeting_id"),
            "platform": claims.get("platform"),
            "segments": [body["segment"]],
        }
        try:
            accepted = await ingest(store, redis, {"payload": json.dumps(payload)})
        except Exception:
            return _json({"detail": "transcript ingress unavailable; retry"}, 503)
        if accepted != 1:
            return _json({"detail": "meeting transcript is no longer writable"}, 409)
        return _json({"accepted": True}, 202)

    @router.post("/bots/internal/transcripts/fence", include_in_schema=False)
    async def fence_managed_transcript(
        request: Request,
        authorization: Optional[str] = Header(default=None),
    ):
        if token_secret is None or carrier_fencer is None:
            return _json({"detail": "transcript fence unavailable; retry"}, 503)
        claims, denial = _verify_token(
            authorization=authorization,
            token_secret=token_secret,
        )
        if denial is not None:
            return denial
        assert claims is not None
        try:
            body = await _body(request)
        except OverflowError:
            return _json({"detail": "request body is too large"}, 413)
        except ValueError as error:
            return _json({"detail": str(error)}, 400)
        if (
            set(body) != _FENCE_FIELDS
            or body.get("raw") is not True
            or body.get("processed") is not True
        ):
            return _json({"detail": "off-contract transcript fence"}, 422)
        denial = await _bind_session(
            claims=claims,
            connection_id=body.get("connection_id"),
            meeting_repo=meeting_repo,
        )
        if denial is not None:
            return denial
        try:
            await carrier_fencer(claims["meeting_id"], raw=True, processed=True)
        except Exception:
            return _json({"detail": "transcript fence unavailable; retry"}, 503)
        return _json({"fenced": True}, 200)

    return router
