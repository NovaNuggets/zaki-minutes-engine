import hashlib
import hmac
import json

from gateway.identity_proof import GatewayIdentitySigner


SECRET = "gateway-request-signing-secret-0123456789abcdef"


def test_gateway_identity_proof_is_request_bound_and_never_transmits_master_secret():
    signer = GatewayIdentitySigner(
        SECRET,
        now=lambda: 1_752_576_000,
        nonce=lambda: "nonce-0123456789abcdef012345",
    )

    headers = signer.headers(
        method="POST",
        path="/api/minutes/summarize-last",
        user_id="7",
        identity_headers={"x-user-id": "7"},
        query="",
        body=b"",
    )

    identity_digest = hashlib.sha256(json.dumps(
        {
            "content-type": None,
            "last-event-id": None,
            "x-user-email": None,
            "x-user-id": "7",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()).hexdigest()
    content_digest = hashlib.sha256(b"").hexdigest()

    assert headers == {
        "x-gateway-key-id": hashlib.sha256(SECRET.encode()).hexdigest()[:16],
        "x-gateway-timestamp": "1752576000",
        "x-gateway-nonce": "nonce-0123456789abcdef012345",
        "x-gateway-content-sha256": content_digest,
        "x-gateway-signature": hmac.new(
            SECRET.encode(),
            (
                "gateway-request.v1\nPOST\n/api/minutes/summarize-last\n7\n\n"
                f"{content_digest}\n{identity_digest}\n1752576000\n"
                "nonce-0123456789abcdef012345"
            ).encode(),
            hashlib.sha256,
        ).hexdigest(),
    }
    assert SECRET not in headers.values()


def test_gateway_identity_signer_rejects_noncanonical_inputs():
    signer = GatewayIdentitySigner(SECRET)

    for method, path, user_id in (
        ("", "/api/sessions", "7"),
        ("GET\nPOST", "/api/sessions", "7"),
        ("GET", "api/sessions", "7"),
        ("GET", "/api/sessions\nforged", "7"),
        ("GET", "/api/sessions", "attacker"),
    ):
        try:
            signer.headers(method=method, path=path, user_id=user_id)
        except ValueError:
            pass
        else:
            raise AssertionError("noncanonical identity proof input was accepted")

    try:
        signer.headers(
            method="GET",
            path="/api/sessions",
            user_id="7",
            identity_headers={"x-user-id": "7", "x-user-email": "café@example.com"},
        )
    except ValueError:
        pass
    else:
        raise AssertionError("non-ASCII transport header was accepted")


def test_gateway_identity_proof_binds_query_body_and_control_headers():
    nonces = iter((
        "nonce-identity-0123456789abcdef",
        "nonce-query-0123456789abcdef012",
        "nonce-body-0123456789abcdef0123",
    ))
    signer = GatewayIdentitySigner(
        SECRET,
        now=lambda: 1_752_576_000,
        nonce=lambda: next(nonces),
    )
    base = dict(
        method="POST",
        path="/api/workspace/invites/accept",
        user_id="7",
    )
    first = signer.headers(
        **base,
        query="mode=restricted",
        body=b'{"token":"one"}',
        identity_headers={
            "x-user-id": "7",
            "x-user-email": "owner@example.com",
            "content-type": "application/json",
        },
    )
    query_changed = signer.headers(
        **base,
        query="mode=owner",
        body=b'{"token":"one"}',
        identity_headers={
            "x-user-id": "7",
            "x-user-email": "owner@example.com",
            "content-type": "application/json",
        },
    )
    body_changed = signer.headers(
        **base,
        query="mode=restricted",
        body=b'{"token":"two"}',
        identity_headers={
            "x-user-id": "7",
            "x-user-email": "attacker@example.com",
            "content-type": "application/json",
        },
    )

    assert len({
        first["x-gateway-signature"],
        query_changed["x-gateway-signature"],
        body_changed["x-gateway-signature"],
    }) == 3
    assert first["x-gateway-content-sha256"] != body_changed["x-gateway-content-sha256"]
