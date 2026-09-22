"""Cross-version compatibility guards for identity.v1 → identity.v2."""
import json
from pathlib import Path

import jsonschema
from referencing import Registry, Resource


ROOT = Path(__file__).resolve().parents[1]


def _validator(version: str):
    schema = json.loads(
        (ROOT / "contracts" / version / "identity.schema.json").read_text()
    )
    registry = Registry().with_resource(schema["$id"], Resource.from_contents(schema))
    return jsonschema.Draft202012Validator(
        {"$ref": f"{schema['$id']}#/$defs/ScopedToken"}, registry=registry
    )


def test_old_v1_consumer_rejects_v2_agent_token_while_v2_accepts_it():
    value = {
        "subject": "9223372036854775807",
        "scopes": ["agent"],
        "expires_at": None,
    }
    assert list(_validator("identity.v1").iter_errors(value))
    assert list(_validator("identity.v2").iter_errors(value)) == []


def test_v2_principal_is_never_a_lossy_json_number():
    value = {"subject": 9_007_199_254_740_993, "scopes": ["agent"], "expires_at": None}
    assert list(_validator("identity.v2").iter_errors(value))
