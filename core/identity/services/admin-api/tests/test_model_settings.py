"""Settings → Models (identity side) — /user/models + /user/transcription self-serve, the
platform_settings internal CRUD, and the resolution edges dispatch/bot_spawn consume.

Secrets (api_key, transcription token) are masked on every user-facing read and cross in the
clear ONLY over the X-Internal-Secret edges (`/internal/users/{id}/model-config`, bot-context).
Models resolve field-by-field; transcription selects one atomic user/platform backend tier.

Same testcontainers-PG harness as O-STACK-3 (skips without docker).
"""
import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from admin_api.app import db as app_db
from admin_api.app.main import create_app
from admin_api.schema.models import Base
from admin_api.schema.sync import ensure_schema_sync

from conftest import requires_docker
from test_stack_admin_api import ADMIN_TOKEN, INTERNAL_SECRET, _admin, _dispose_async_engine

pytestmark = requires_docker


@pytest.fixture()
def client(pg_url, pg_async_url, monkeypatch):
    sync_engine = create_engine(pg_url)
    Base.metadata.drop_all(sync_engine)
    ensure_schema_sync(sync_engine, Base)
    sync_engine.dispose()
    monkeypatch.setenv("ADMIN_API_TOKEN", ADMIN_TOKEN)
    monkeypatch.setenv("INTERNAL_API_SECRET", INTERNAL_SECRET)
    monkeypatch.setenv("DEV_MODE", "false")
    app_db.configure(pg_async_url)
    with TestClient(create_app()) as c:
        yield c
    _dispose_async_engine()


def _internal():
    return {"X-Internal-Secret": INTERNAL_SECRET}


def _user_token(client, email="models@vexa.ai"):
    uid = client.post("/admin/users", headers=_admin(), json={"email": email}).json()["id"]
    tok = client.post(f"/admin/users/{uid}/tokens?scopes=bot", headers=_admin()).json()["token"]
    return uid, tok


def test_user_models_set_masked_readback_and_clear(client, monkeypatch):
    _uid, tok = _user_token(client)
    h = {"X-API-Key": tok}
    monkeypatch.setenv("VEXA_USER_MODEL_ALLOWED_HOSTS", "llm.example.com")

    r = client.put("/user/models", headers=h, json={
        "mode": "custom", "model": "qwen3-coder", "base_url": "https://llm.example.com/v1",
        "api_key": "sk-secret-1234abcd",
    })
    assert r.status_code == 200, r.text
    cfg = r.json()
    assert cfg["mode"] == "custom"
    assert cfg["model"] == "qwen3-coder"
    assert cfg["api_key_set"] is True
    # masked: the secret NEVER echoes in the clear
    assert "sk-secret" not in (cfg["api_key"] or "")
    assert cfg["api_key"].endswith("abcd")

    # partial update leaves other fields; empty string clears one
    r = client.put("/user/models", headers=h, json={"model": ""})
    cfg = r.json()
    assert cfg["model"] is None
    assert cfg["mode"] == "custom"          # untouched
    assert cfg["api_key_set"] is True       # untouched


def test_user_models_validation(client):
    _uid, tok = _user_token(client, email="val@vexa.ai")
    h = {"X-API-Key": tok}
    assert client.put("/user/models", headers=h, json={"mode": "yolo"}).status_code == 422
    assert client.put("/user/models", headers=h, json={"base_url": "not-a-url"}).status_code == 422
    assert client.put("/user/transcription", headers=h, json={"url": "ftp://x"}).status_code == 422


def test_user_custom_model_backend_never_inherits_platform_credentials(client, monkeypatch):
    uid, tok = _user_token(client, email="model-atomic@example.com")
    client.put(
        "/internal/settings/models",
        headers=_internal(),
        json={
            "mode": "custom",
            "base_url": "http://operator-model:8080",
            "api_key": "operator-model-secret",
            "model": "operator-model",
        },
    )
    monkeypatch.setenv("VEXA_USER_MODEL_ALLOWED_HOSTS", "user-model.example")
    user = client.put(
        "/user/models",
        headers={"X-API-Key": tok},
        json={
            "mode": "custom",
            "base_url": "https://user-model.example/v1",
            "model": "user-model",
        },
    )
    assert user.status_code == 200, user.text

    resolved = client.get(f"/internal/users/{uid}/model-config", headers=_internal()).json()["models"]
    assert resolved == {
        "mode": "custom",
        "base_url": "https://user-model.example/v1",
        "model": "user-model",
    }
    assert "operator-model-secret" not in json.dumps(resolved)


def test_user_model_url_rotation_clears_stale_key_and_subscription_stays_managed(client, monkeypatch):
    uid, tok = _user_token(client, email="model-rotate@example.com")
    headers = {"X-API-Key": tok}
    monkeypatch.setenv(
        "VEXA_USER_MODEL_ALLOWED_HOSTS",
        "model-one.example,model-two.example",
    )
    first = client.put(
        "/user/models",
        headers=headers,
        json={
            "mode": "custom",
            "base_url": "https://model-one.example/v1",
            "api_key": "first-model-key",
        },
    )
    assert first.status_code == 200, first.text
    rotated = client.put(
        "/user/models",
        headers=headers,
        json={"base_url": "https://model-two.example/v1"},
    )
    assert rotated.status_code == 200, rotated.text
    assert rotated.json()["api_key_set"] is False
    resolved = client.get(f"/internal/users/{uid}/model-config", headers=_internal()).json()["models"]
    assert resolved["base_url"] == "https://model-two.example/v1"
    assert "api_key" not in resolved

    client.put(
        "/internal/settings/models",
        headers=_internal(),
        json={
            "mode": "custom",
            "base_url": "http://operator-model:8080",
            "api_key": "operator-model-secret",
        },
    )
    subscription = client.put("/user/models", headers=headers, json={"mode": "subscription"})
    assert subscription.status_code == 200
    managed = client.get(f"/internal/users/{uid}/model-config", headers=_internal()).json()["models"]
    assert managed["mode"] == "subscription"
    assert "base_url" not in managed and "api_key" not in managed


def test_user_model_mode_transitions_remove_hidden_custom_provider_state(client, monkeypatch):
    uid, tok = _user_token(client, email="model-mode-transition@example.com")
    headers = {"X-API-Key": tok}
    monkeypatch.setenv("VEXA_USER_MODEL_ALLOWED_HOSTS", "user-model.example")

    custom = {
        "mode": "custom",
        "base_url": "https://user-model.example/v1",
        "api_key": "user-model-key",
        "model": "user-model",
    }
    assert client.put("/user/models", headers=headers, json=custom).status_code == 200

    defaulted = client.put("/user/models", headers=headers, json={"mode": ""})
    assert defaulted.status_code == 200, defaulted.text
    assert defaulted.json() == {
        "mode": None,
        "model": "user-model",
        "meeting_model": None,
        "base_url": None,
            "api_key_set": False,
            "api_key": None,
            "config_status": "valid",
            "validation_error": None,
        }
    resolved_default = client.get(
        f"/internal/users/{uid}/model-config", headers=_internal(),
    ).json()["models"]
    assert resolved_default == {"model": "user-model"}
    assert "user-model-key" not in json.dumps(resolved_default)

    assert client.put("/user/models", headers=headers, json=custom).status_code == 200
    subscribed = client.put("/user/models", headers=headers, json={"mode": "subscription"})
    assert subscribed.status_code == 200, subscribed.text
    assert subscribed.json()["base_url"] is None
    assert subscribed.json()["api_key_set"] is False
    resolved_subscription = client.get(
        f"/internal/users/{uid}/model-config", headers=_internal(),
    ).json()["models"]
    assert resolved_subscription == {"mode": "subscription", "model": "user-model"}
    assert "user-model-key" not in json.dumps(resolved_subscription)


@pytest.mark.parametrize("url", [
    "http://model.example",
    "https://localhost",
    "https://127.0.0.1",
    "https://127.1",
    "https://169.254.169.254/latest/meta-data",
    "https://metadata.google.internal",
    "https://model.example:4443/v1",
    "https://user:pass@model.example/v1",
    "https://model.example/v1?target=internal",
    "https://model.example/v1#fragment",
    "https://-1.0.0.1",
])
def test_user_model_endpoint_ssrf_and_origin_rules_fail_closed(client, monkeypatch, url):
    from urllib.parse import urlparse

    _uid, tok = _user_token(client, email=f"model-ssrf-{abs(hash(url))}@example.com")
    monkeypatch.setenv("VEXA_USER_MODEL_ALLOWED_HOSTS", urlparse(url).hostname or "")
    response = client.put(
        "/user/models",
        headers={"X-API-Key": tok},
        json={"mode": "custom", "base_url": url},
    )
    assert response.status_code == 422, f"{url}: {response.status_code} {response.text}"


def test_user_model_endpoint_is_default_deny_and_exactly_allowlisted(client, monkeypatch):
    _uid, tok = _user_token(client, email="model-allowlist@example.com")
    headers = {"X-API-Key": tok}
    monkeypatch.delenv("VEXA_USER_MODEL_ALLOWED_HOSTS", raising=False)
    denied = client.put(
        "/user/models",
        headers=headers,
        json={"mode": "custom", "base_url": "https://model.example/v1"},
    )
    assert denied.status_code == 422

    monkeypatch.setenv("VEXA_USER_MODEL_ALLOWED_HOSTS", "model.example")
    allowed = client.put(
        "/user/models",
        headers=headers,
        json={"mode": "custom", "base_url": "https://model.example/v1"},
    )
    assert allowed.status_code == 200, allowed.text


def test_user_model_allowlist_revocation_blocks_without_switching_origin(client, monkeypatch):
    uid, tok = _user_token(client, email="model-revoked@example.com")
    client.put(
        "/internal/settings/models",
        headers=_internal(),
        json={
            "mode": "custom",
            "base_url": "http://operator-model:8080",
            "api_key": "operator-key",
        },
    )
    monkeypatch.setenv("VEXA_USER_MODEL_ALLOWED_HOSTS", "user-model.example")
    saved = client.put(
        "/user/models",
        headers={"X-API-Key": tok},
        json={
            "mode": "custom",
            "base_url": "https://user-model.example/v1",
            "api_key": "user-key",
        },
    )
    assert saved.status_code == 200

    monkeypatch.delenv("VEXA_USER_MODEL_ALLOWED_HOSTS")
    resolved = client.get(f"/internal/users/{uid}/model-config", headers=_internal()).json()["models"]
    assert resolved == {
        "mode": "custom",
        "model": None,
        "meeting_model": None,
        "blocked": True,
        "config_status": "blocked",
        "validation_error": "Personal model endpoint is no longer operator-approved.",
    }
    assert "operator-key" not in json.dumps(resolved)
    assert "user-key" not in json.dumps(resolved)

    visible = client.get("/user/models", headers={"X-API-Key": tok}).json()
    assert visible["base_url"] == "https://user-model.example/v1"
    assert visible["config_status"] == "blocked"
    assert "operator-approved" in visible["validation_error"]


@pytest.mark.parametrize("url", [
    "http://stt.example.com",
    "https://localhost:8080",
    "https://admin-api:8080",
    "https://127.0.0.1:8080",
    "https://127.1:8080",
    "https://10.0.0.8:8080",
    "https://169.254.169.254/latest/meta-data",
    "https://[::1]:8080",
    "https://metadata.google.internal",
])
def test_user_transcription_rejects_ssrf_targets(client, monkeypatch, url):
    from urllib.parse import urlparse

    _uid, tok = _user_token(client, email=f"ssrf-{abs(hash(url))}@vexa.ai")
    monkeypatch.setenv(
        "VEXA_USER_TRANSCRIPTION_ALLOWED_HOSTS",
        urlparse(url).hostname or "",
    )

    response = client.put(
        "/user/transcription",
        headers={"X-API-Key": tok},
        json={"url": url},
    )

    assert response.status_code == 422, f"{url}: {response.status_code} {response.text}"


def test_user_transcription_public_hosts_are_operator_allowlisted(client, monkeypatch):
    _uid, tok = _user_token(client, email="stt-allowlist@vexa.ai")
    headers = {"X-API-Key": tok}
    monkeypatch.delenv("VEXA_USER_TRANSCRIPTION_ALLOWED_HOSTS", raising=False)

    denied = client.put(
        "/user/transcription",
        headers=headers,
        json={"url": "https://attacker-controlled.example"},
    )
    assert denied.status_code == 422, denied.text

    monkeypatch.setenv("VEXA_USER_TRANSCRIPTION_ALLOWED_HOSTS", "stt.user.example")
    allowed = client.put(
        "/user/transcription",
        headers=headers,
        json={"url": "https://stt.user.example/v1"},
    )
    assert allowed.status_code == 200, allowed.text

    explicit_default_port = client.put(
        "/user/transcription",
        headers=headers,
        json={"url": "https://stt.user.example:443/v1"},
    )
    assert explicit_default_port.status_code == 200, explicit_default_port.text


@pytest.mark.parametrize("url", [
    "https://stt.user.example:4443/v1",
    "https://stt.user.example/v1?redirect=https://internal.example",
    "https://stt.user.example/v1#alternate",
])
def test_user_transcription_allowlist_does_not_authorize_another_origin_or_url_mode(
    client, monkeypatch, url
):
    _uid, tok = _user_token(client, email=f"stt-origin-{abs(hash(url))}@vexa.ai")
    monkeypatch.setenv("VEXA_USER_TRANSCRIPTION_ALLOWED_HOSTS", "stt.user.example")

    response = client.put(
        "/user/transcription",
        headers={"X-API-Key": tok},
        json={"url": url},
    )

    assert response.status_code == 422, f"{url}: {response.status_code} {response.text}"


def test_user_transcription_url_change_does_not_reuse_prior_endpoint_token(client, monkeypatch):
    uid, tok = _user_token(client, email="stt-rotate@vexa.ai")
    headers = {"X-API-Key": tok}
    monkeypatch.setenv(
        "VEXA_USER_TRANSCRIPTION_ALLOWED_HOSTS",
        "stt-one.example,stt-two.example",
    )
    first = client.put(
        "/user/transcription",
        headers=headers,
        json={"url": "https://stt-one.example", "token": "first-endpoint-token"},
    )
    assert first.status_code == 200, first.text

    changed = client.put(
        "/user/transcription",
        headers=headers,
        json={"url": "https://stt-two.example"},
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["token_set"] is False

    context = client.get(f"/internal/users/{uid}/bot-context", headers=_internal())
    assert context.json()["transcription"] == {"url": "https://stt-two.example"}


@pytest.mark.parametrize("url", [
    "https://[::1",
    "https://stt.user.example:not-a-port",
    "https://-1",
    "https://-1.0.0.1",
])
def test_user_transcription_malformed_urls_fail_closed(client, monkeypatch, url):
    _uid, tok = _user_token(client, email=f"stt-malformed-{abs(hash(url))}@vexa.ai")
    monkeypatch.setenv("VEXA_USER_TRANSCRIPTION_ALLOWED_HOSTS", "stt.user.example")

    response = client.put(
        "/user/transcription",
        headers={"X-API-Key": tok},
        json={"url": url},
    )

    assert response.status_code == 422, f"{url}: {response.status_code} {response.text}"


def test_platform_settings_crud_and_gate(client):
    # internal edge only — no/wrong secret fails closed
    assert client.get("/internal/settings/models").status_code == 403
    assert client.put("/internal/settings/models",
                      headers={"X-Internal-Secret": "wrong"}, json={}).status_code == 403
    # unknown key 404s
    assert client.get("/internal/settings/nope", headers=_internal()).status_code == 404

    r = client.put("/internal/settings/models", headers=_internal(),
                   json={"model": "haiku", "mode": "subscription"})
    assert r.status_code == 200, r.text
    assert r.json()["value"] == {"model": "haiku", "mode": "subscription"}
    # partial update + clear
    r = client.put("/internal/settings/models", headers=_internal(), json={"mode": ""})
    assert r.json()["value"] == {"model": "haiku"}
    assert client.get("/internal/settings/models", headers=_internal()).json()["value"] == {"model": "haiku"}
    # same field rules as the user tier
    assert client.put("/internal/settings/models", headers=_internal(),
                      json={"mode": "yolo"}).status_code == 422

    # Operator-owned backends may deliberately use the deployment's in-cluster HTTP service.
    r = client.put(
        "/internal/settings/transcription",
        headers=_internal(),
        json={"url": "http://transcription-service:8000", "token": "operator-token"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["value"] == {
        "url": "http://transcription-service:8000",
        "token": "operator-token",
    }


def test_platform_transcription_url_rotation_clears_only_an_omitted_stale_token(client):
    first = client.put(
        "/internal/settings/transcription",
        headers=_internal(),
        json={"url": "http://stt-one:8000", "token": "first-origin-token"},
    )
    assert first.status_code == 200, first.text

    rotated = client.put(
        "/internal/settings/transcription",
        headers=_internal(),
        json={"url": "http://stt-two:8000"},
    )
    assert rotated.status_code == 200, rotated.text
    assert rotated.json()["value"] == {"url": "http://stt-two:8000"}

    replaced = client.put(
        "/internal/settings/transcription",
        headers=_internal(),
        json={"token": "second-origin-token"},
    )
    assert replaced.json()["value"] == {
        "url": "http://stt-two:8000",
        "token": "second-origin-token",
    }

    unchanged_url = client.put(
        "/internal/settings/transcription",
        headers=_internal(),
        json={"url": "http://stt-two:8000"},
    )
    assert unchanged_url.json()["value"]["token"] == "second-origin-token"

    cleared = client.put(
        "/internal/settings/transcription",
        headers=_internal(),
        json={"token": ""},
    )
    assert cleared.json()["value"] == {"url": "http://stt-two:8000"}


def test_platform_model_url_rotation_clears_only_an_omitted_stale_api_key(client):
    first = client.put(
        "/internal/settings/models",
        headers=_internal(),
        json={"base_url": "http://model-one:8080", "api_key": "first-origin-key"},
    )
    assert first.status_code == 200, first.text

    rotated = client.put(
        "/internal/settings/models",
        headers=_internal(),
        json={"base_url": "http://model-two:8080"},
    )
    assert rotated.status_code == 200, rotated.text
    assert rotated.json()["value"] == {"base_url": "http://model-two:8080"}

    replaced = client.put(
        "/internal/settings/models",
        headers=_internal(),
        json={"api_key": "second-origin-key"},
    )
    assert replaced.json()["value"] == {
        "base_url": "http://model-two:8080",
        "api_key": "second-origin-key",
    }

    unchanged_url = client.put(
        "/internal/settings/models",
        headers=_internal(),
        json={"base_url": "http://model-two:8080"},
    )
    assert unchanged_url.json()["value"]["api_key"] == "second-origin-key"

    cleared = client.put(
        "/internal/settings/models",
        headers=_internal(),
        json={"api_key": ""},
    )
    assert cleared.json()["value"] == {"base_url": "http://model-two:8080"}


def test_platform_model_mode_transitions_remove_hidden_custom_provider_state(client):
    custom = {
        "mode": "custom",
        "base_url": "http://operator-model:8080",
        "api_key": "operator-model-key",
        "model": "operator-model",
    }
    assert client.put(
        "/internal/settings/models", headers=_internal(), json=custom,
    ).status_code == 200

    defaulted = client.put(
        "/internal/settings/models", headers=_internal(), json={"mode": ""},
    )
    assert defaulted.status_code == 200, defaulted.text
    assert defaulted.json()["value"] == {"model": "operator-model"}

    uid, _tok = _user_token(client, email="platform-mode-transition@example.com")
    resolved_default = client.get(
        f"/internal/users/{uid}/model-config", headers=_internal(),
    ).json()["models"]
    assert resolved_default == {"model": "operator-model"}
    assert "operator-model-key" not in json.dumps(resolved_default)

    assert client.put(
        "/internal/settings/models", headers=_internal(), json=custom,
    ).status_code == 200
    subscribed = client.put(
        "/internal/settings/models", headers=_internal(), json={"mode": "subscription"},
    )
    assert subscribed.status_code == 200, subscribed.text
    assert subscribed.json()["value"] == {
        "mode": "subscription",
        "model": "operator-model",
    }
    resolved_subscription = client.get(
        f"/internal/users/{uid}/model-config", headers=_internal(),
    ).json()["models"]
    assert resolved_subscription == {
        "mode": "subscription",
        "model": "operator-model",
    }
    assert "operator-model-key" not in json.dumps(resolved_subscription)


def test_model_config_resolves_user_over_platform(client, monkeypatch):
    uid, tok = _user_token(client, email="resolve@vexa.ai")
    client.put("/internal/settings/models", headers=_internal(),
               json={"model": "global-model", "meeting_model": "global-meeting",
                     "mode": "custom", "base_url": "https://global.example.com",
                     "api_key": "sk-platform-key"})
    client.put("/user/models", headers={"X-API-Key": tok},
               json={"model": "my-model", "api_key": "sk-user-key"})

    r = client.get(f"/internal/users/{uid}/model-config", headers=_internal())
    assert r.status_code == 200, r.text
    models = r.json()["models"]
    assert r.json()["credential_owner"] == "operator"
    assert models["model"] == "my-model"                       # user beats platform
    assert models["meeting_model"] == "global-meeting"         # platform fills the gap
    assert models["base_url"] == "https://global.example.com"
    # Model names may be personalized, but endpoint credentials are an atomic owner tier:
    # an orphan user key must never replace the platform endpoint's key.
    assert models["api_key"] == "sk-platform-key"
    assert "sk-user-key" not in json.dumps(models)

    # unknown subject → 404 (dispatch treats it as env defaults)
    assert client.get("/internal/users/999999/model-config", headers=_internal()).status_code == 404

    # Selecting a complete personal custom provider moves only that credential bundle to the user.
    monkeypatch.setenv("VEXA_USER_MODEL_ALLOWED_HOSTS", "personal.example.com")
    client.put(
        "/user/models",
        headers={"X-API-Key": tok},
        json={
            "mode": "custom",
            "base_url": "https://personal.example.com",
            "api_key": "sk-personal-key",
        },
    )
    personal = client.get(f"/internal/users/{uid}/model-config", headers=_internal()).json()
    assert personal["credential_owner"] == "user"

    # Subscription is a user preference over operator/deployment credentials, not a personal key.
    client.put("/user/models", headers={"X-API-Key": tok}, json={"mode": "subscription"})
    managed = client.get(f"/internal/users/{uid}/model-config", headers=_internal()).json()
    assert managed["credential_owner"] == "operator"


def test_bot_context_carries_effective_transcription(client, monkeypatch):
    uid, tok = _user_token(client, email="stt@vexa.ai")
    # nothing configured → no transcription key at all (bot_spawn keeps its env)
    r = client.get(f"/internal/users/{uid}/bot-context", headers=_internal())
    assert "transcription" not in r.json()
    assert r.json()["transcription_credential_owner"] == "operator"

    client.put("/internal/settings/transcription", headers=_internal(),
               json={"url": "https://stt-global.example.com", "token": "tok-global"})
    r = client.get(f"/internal/users/{uid}/bot-context", headers=_internal())
    assert r.json()["transcription"] == {"url": "https://stt-global.example.com", "token": "tok-global"}
    assert r.json()["transcription_credential_owner"] == "operator"

    monkeypatch.setenv("VEXA_USER_TRANSCRIPTION_ALLOWED_HOSTS", "stt-mine.example.com")
    client.put("/user/transcription", headers={"X-API-Key": tok},
               json={"url": "https://stt-mine.example.com"})
    r = client.get(f"/internal/users/{uid}/bot-context", headers=_internal())
    # A backend is one credential boundary: a user URL never inherits the platform token.
    assert r.json()["transcription"] == {"url": "https://stt-mine.example.com"}
    assert r.json()["transcription_credential_owner"] == "user"

    # Operator allowlist revocation takes effect on read without silently switching origins.
    monkeypatch.delenv("VEXA_USER_TRANSCRIPTION_ALLOWED_HOSTS")
    r = client.get(f"/internal/users/{uid}/bot-context", headers=_internal())
    assert r.json()["transcription"] == {
        "blocked": True,
        "config_status": "blocked",
        "validation_error": "Personal transcription endpoint is no longer operator-approved.",
    }

    # masked user-facing read-back
    cfg = client.get("/user/transcription", headers={"X-API-Key": tok}).json()
    assert cfg["url"] == "https://stt-mine.example.com"
    assert cfg["token_set"] is False
    assert cfg["config_status"] == "blocked"
    assert "operator-approved" in cfg["validation_error"]
