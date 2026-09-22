"""Static image packaging contract for the server-owned Minutes ingestion path."""
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_agent_api_image_packages_isolated_llm_and_sealed_read_schema():
    dockerfile = (ROOT / "core/agent/services/agent-api/Dockerfile").read_text()

    assert "COPY core/agent/llm ./llm" in dockerfile
    assert (
        "COPY core/meetings/contracts/zaki-read.v1/zaki-read.schema.json"
        in dockerfile
    )


def test_lite_agent_tree_packages_the_same_sealed_read_schema():
    dockerfile = (ROOT / "deploy/lite/Dockerfile.lite").read_text()

    assert (
        "COPY core/meetings/contracts/zaki-read.v1/zaki-read.schema.json"
        in dockerfile
    )
    assert "/app/agent/meetings/contracts/zaki-read.v1/zaki-read.schema.json" in dockerfile
