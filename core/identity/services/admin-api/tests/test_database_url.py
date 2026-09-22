"""Production database URL composition from operator-owned Secret values."""

from __future__ import annotations

import pytest

from admin_api import __main__ as entry


def test_database_url_encodes_raw_operator_secret_components(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "db.example.internal")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.setenv("DB_NAME", "identity/prod")
    monkeypatch.setenv("DB_USER", "user@tenant")
    monkeypatch.setenv("DB_PASSWORD", "p/a%s@s:word")

    assert entry._database_url() == (
        "postgresql+asyncpg://user%40tenant:p%2Fa%25s%40s%3Aword@"
        "db.example.internal:5433/identity%2Fprod"
    )


def test_database_url_preserves_an_explicit_operator_url(monkeypatch):
    explicit = "postgresql+asyncpg://brokered:opaque@db.example.internal/identity"
    monkeypatch.setenv("DATABASE_URL", explicit)
    assert entry._database_url() == explicit


def test_database_url_applies_the_operator_tls_mode(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_SSL_MODE", "verify-full")

    assert entry._database_url().endswith("/vexa?ssl=verify-full")


def test_database_url_rejects_an_unknown_tls_mode(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_SSL_MODE", "prefer")

    with pytest.raises(RuntimeError, match="DB_SSL_MODE"):
        entry._database_url()
