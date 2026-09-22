"""Production SQLAlchemy mirror must keep each table's declarations on the right model."""
from __future__ import annotations

import ast
from pathlib import Path


def _class_body(name: str) -> list[ast.stmt]:
    source = Path(__file__).parents[1] / "src" / "meeting_api" / "sessions" / "models.py"
    module = ast.parse(source.read_text())
    return next(node.body for node in module.body if isinstance(node, ast.ClassDef) and node.name == name)


def test_minutes_receipt_does_not_steal_meeting_properties_or_indexes():
    meeting = _class_body("Meeting")
    receipt = _class_body("MinutesErasureReceipt")

    def assigned(body: list[ast.stmt], name: str) -> list[ast.stmt]:
        return [
            node for node in body
            if isinstance(node, (ast.Assign, ast.AnnAssign))
            and any(isinstance(target, ast.Name) and target.id == name
                    for target in (node.targets if isinstance(node, ast.Assign) else [node.target]))
        ]

    assert any(isinstance(node, ast.FunctionDef) and node.name == "native_meeting_id" for node in meeting)
    assert len(assigned(meeting, "__table_args__")) == 1
    assert not any(isinstance(node, ast.FunctionDef) and node.name == "native_meeting_id" for node in receipt)
    assert len(assigned(receipt, "__table_args__")) == 1
