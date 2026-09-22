import hashlib
import hmac
import importlib.util
import json
from pathlib import Path
import urllib.parse

import pytest

from control_plane.gateway_identity import (
    GatewayReplayUnavailable,
    GatewayIdentityVerifier,
    InMemoryGatewayReplayStore,
    RedisGatewayReplayStore,
)
from control_plane.api import _security_redis_from_url


CURRENT = "gateway-current-signing-secret-0123456789abcdef"
PREVIOUS = "gateway-previous-signing-secret-0123456789abcdef"
NOW = 1_752_576_000
_SIGNED_HEADERS = ("content-type", "last-event-id", "x-user-email", "x-user-id")


def _headers(secret=CURRENT, *, method="POST", path="/api/minutes/summarize-last",
             user_id="7", timestamp=NOW, nonce="nonce-0123456789abcdef012345",
             query="", body=b"", identity=None):
    identity = {**(identity or {}), "x-user-id": user_id}
    identity_payload = {
        name: identity.get(name) for name in _SIGNED_HEADERS
    }
    identity_digest = hashlib.sha256(json.dumps(
        identity_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    canonical_query = urllib.parse.urlencode(urllib.parse.parse_qsl(
        query, keep_blank_values=True,
    ))
    content_digest = hashlib.sha256(body).hexdigest()
    canonical = (
        f"gateway-request.v1\n{method}\n{path}\n{user_id}\n{canonical_query}\n"
        f"{content_digest}\n{identity_digest}\n{timestamp}\n{nonce}"
    ).encode("utf-8")
    return {
        **identity,
        "x-gateway-key-id": hashlib.sha256(secret.encode()).hexdigest()[:16],
        "x-gateway-timestamp": str(timestamp),
        "x-gateway-nonce": nonce,
        "x-gateway-content-sha256": content_digest,
        "x-gateway-signature": hmac.new(secret.encode(), canonical, hashlib.sha256).hexdigest(),
    }


def test_verifier_binds_method_path_and_user_and_rejects_replay():
    verifier = GatewayIdentityVerifier(
        CURRENT,
        replay_store=InMemoryGatewayReplayStore(now=lambda: NOW),
        now=lambda: NOW,
    )
    headers = _headers()

    assert verifier.verify(
        method="POST", path="/api/minutes/summarize-last", user_id="7", headers=headers,
    ) is True
    assert verifier.verify(
        method="POST", path="/api/minutes/summarize-last", user_id="7", headers=headers,
    ) is False

    for method, path, user_id in (
        ("GET", "/api/minutes/summarize-last", "7"),
        ("POST", "/api/sessions", "7"),
        ("POST", "/api/minutes/summarize-last", "8"),
    ):
        fresh = _headers(method="POST", path="/api/minutes/summarize-last", user_id="7",
                         nonce=f"nonce-{method}-{user_id}-0123456789")
        assert verifier.verify(method=method, path=path, user_id=user_id, headers=fresh) is False


def test_verifier_accepts_the_utf8_path_canonical_form_used_by_the_gateway():
    verifier = GatewayIdentityVerifier(
        CURRENT,
        replay_store=InMemoryGatewayReplayStore(now=lambda: NOW),
        now=lambda: NOW,
    )

    assert verifier.verify(
        method="PATCH",
        path="/api/routines/café/enabled",
        user_id="7",
        headers=_headers(
            method="PATCH",
            path="/api/routines/café/enabled",
            nonce="unicode-path-0123456789abcdef",
        ),
    ) is True


def test_verifier_binds_query_body_and_every_gateway_identity_attribute():
    verifier = GatewayIdentityVerifier(
        CURRENT,
        replay_store=InMemoryGatewayReplayStore(now=lambda: NOW),
        now=lambda: NOW,
    )
    body = b'{"allowed":true}'
    identity = {
        "x-user-email": "owner@example.com",
        "content-type": "application/json",
        "last-event-id": "cursor-1",
    }
    headers = _headers(
        method="POST",
        path="/api/workspace/invites/accept",
        query="mode=restricted",
        body=body,
        identity=identity,
        nonce="full-request-0123456789abcdef",
    )

    for query, candidate_body, overrides in (
        ("mode=owner", body, {}),
        ("mode=restricted", b'{"allowed":false}', {}),
        ("mode=restricted", body, {"x-user-email": "attacker@example.com"}),
        ("mode=restricted", body, {"last-event-id": "cursor-2"}),
    ):
        assert verifier.verify(
            method="POST",
            path="/api/workspace/invites/accept",
            user_id="7",
            query=query,
            body=candidate_body,
            headers={**headers, **overrides},
        ) is False

    assert verifier.verify(
        method="POST",
        path="/api/workspace/invites/accept",
        user_id="7",
        query="mode=restricted",
        body=body,
        headers=headers,
    ) is True


def test_verifier_accepts_one_previous_rotation_key_and_rejects_stale_or_raw_bearer():
    verifier = GatewayIdentityVerifier(
        CURRENT,
        previous_secret=PREVIOUS,
        replay_store=InMemoryGatewayReplayStore(now=lambda: NOW),
        now=lambda: NOW,
    )

    assert verifier.verify(
        method="POST",
        path="/api/minutes/summarize-last",
        user_id="7",
        headers=_headers(PREVIOUS),
    ) is True
    assert verifier.verify(
        method="POST",
        path="/api/minutes/summarize-last",
        user_id="7",
        headers=_headers(timestamp=NOW - 31, nonce="stale-nonce-0123456789abcdef"),
    ) is False
    assert verifier.verify(
        method="POST",
        path="/api/minutes/summarize-last",
        user_id="7",
        headers={"x-gateway-verified": CURRENT},
    ) is False


def test_redis_replay_claim_is_atomic_and_an_outage_fails_closed():
    class Redis:
        def __init__(self):
            self.calls = []
            self.outcome = True

        def set(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            if isinstance(self.outcome, Exception):
                raise self.outcome
            return self.outcome

    redis = Redis()
    verifier = GatewayIdentityVerifier(
        CURRENT,
        replay_store=RedisGatewayReplayStore(redis),
        now=lambda: NOW,
    )

    assert verifier.verify(
        method="POST",
        path="/api/minutes/summarize-last",
        user_id="7",
        headers=_headers(nonce="redis-claim-0123456789abcdef"),
    ) is True
    assert redis.calls[0][1] == {"nx": True, "ex": 61}

    redis.outcome = RuntimeError("redis unavailable")
    outage_headers = _headers(nonce="redis-outage-0123456789abcdef")
    proof = verifier.authenticate_metadata(
        method="POST",
        path="/api/minutes/summarize-last",
        user_id="7",
        query="",
        headers=outage_headers,
    )
    assert proof is not None
    with pytest.raises(GatewayReplayUnavailable, match="replay fence"):
        verifier.verify_body_and_claim(proof, b"")
    assert verifier.verify(
        method="POST",
        path="/api/minutes/summarize-last",
        user_id="7",
        headers=_headers(nonce="redis-outage-wrapper-0123456789"),
    ) is False


def test_staged_verification_rechecks_freshness_before_claiming_nonce():
    current = [NOW]
    replay = InMemoryGatewayReplayStore(now=lambda: current[0])
    verifier = GatewayIdentityVerifier(
        CURRENT,
        replay_store=replay,
        now=lambda: current[0],
    )
    headers = _headers(body=b"bounded", nonce="slow-body-0123456789abcdef012")
    proof = verifier.authenticate_metadata(
        method="POST",
        path="/api/minutes/summarize-last",
        user_id="7",
        query="",
        headers=headers,
    )
    assert proof is not None

    current[0] += 31
    assert verifier.verify_body_and_claim(proof, b"bounded") is False

    current[0] = NOW
    assert verifier.verify_body_and_claim(proof, b"bounded") is True


def test_verifier_rejects_duplicate_proof_or_signed_control_headers():
    class DuplicateHeaders(dict):
        duplicate = ""

        def getlist(self, name):
            value = self.get(name)
            if name == self.duplicate and value is not None:
                return [value, value]
            return [] if value is None else [value]

    verifier = GatewayIdentityVerifier(
        CURRENT,
        replay_store=InMemoryGatewayReplayStore(now=lambda: NOW),
        now=lambda: NOW,
    )
    base = _headers(identity={"content-type": "application/json"})

    for duplicate in ("x-gateway-signature", "content-type", "x-user-id"):
        headers = DuplicateHeaders(base)
        headers.duplicate = duplicate
        assert verifier.authenticate_metadata(
            method="POST",
            path="/api/minutes/summarize-last",
            user_id="7",
            query="",
            headers=headers,
        ) is None


def test_production_security_redis_has_bounded_connect_and_command_timeouts(monkeypatch):
    import redis

    seen = {}
    sentinel = object()

    def from_url(url, **kwargs):
        seen["url"] = url
        seen["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(redis.Redis, "from_url", from_url)

    assert _security_redis_from_url("redis://security/0") is sentinel
    assert seen == {
        "url": "redis://security/0",
        "kwargs": {
            "decode_responses": True,
            "socket_connect_timeout": 2.0,
            "socket_timeout": 2.0,
            "retry_on_timeout": False,
            "health_check_interval": 30,
        },
    }


def test_real_gateway_signer_and_agent_verifier_share_one_wire_contract():
    signer_path = (
        Path(__file__).resolve().parents[2]
        / "gateway/services/gateway/src/gateway/identity_proof.py"
    )
    spec = importlib.util.spec_from_file_location("gateway_identity_wire", signer_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    body = b'{"prompt":"caf\xc3\xa9"}'
    path = "/api/routines/café/enabled"
    query = "tag=b&tag=a&name=caf%C3%A9"
    identity = {
        "content-type": "application/json",
        "last-event-id": "cursor-42",
        "x-user-email": "owner@example.com",
        "x-user-id": "7",
    }
    signer = module.GatewayIdentitySigner(
        CURRENT,
        now=lambda: NOW,
        nonce=lambda: "cross-side-0123456789abcdef012",
    )
    proof_headers = signer.headers(
        method="PATCH",
        path=path,
        user_id="7",
        query=query,
        body=body,
        identity_headers=identity,
    )
    verifier = GatewayIdentityVerifier(
        CURRENT,
        replay_store=InMemoryGatewayReplayStore(now=lambda: NOW),
        now=lambda: NOW,
    )

    assert verifier.verify(
        method="PATCH",
        path=path,
        user_id="7",
        query=query,
        body=body,
        headers={**identity, **proof_headers},
    ) is True
