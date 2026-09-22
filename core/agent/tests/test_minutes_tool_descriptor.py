"""The Minutes capability descriptor carries policy, never its fleet credential."""
from __future__ import annotations

import json
from pathlib import Path

import contracts


def test_minutes_read_is_a_secret_free_builtin_descriptor():
    path = Path(__file__).parents[1] / "tools-seed" / "minutes-read.json"
    spec = json.loads(path.read_text())

    assert set(spec) == {"tool"}
    contracts.validate_tool(spec["tool"])
    assert spec["tool"] == {
        "name": "minutes_read",
        "scope": "minutes:read",
        "grant": "gate",
        "transport": "builtin",
        "cred_ref": "secret://zaki-read/minutes",
        "barriers": ["sensitive_pii"],
    }
    serialized = json.dumps(spec).lower()
    assert "zaki_read_token_minutes" not in serialized
    assert "mcp" not in serialized
    assert "env" not in serialized
    assert "http://" not in serialized and "https://" not in serialized
