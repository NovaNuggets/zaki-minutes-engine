#!/usr/bin/env python3
"""Retired hosted native-keyed meeting bridge.

This utility used a native meeting id as the transcript carrier and launched an unfenced copilot via
``POST /api/meeting/start``. The managed Minutes path now requires a locally owned numeric meeting row,
an authenticated owner check, and a generation-bound processing grant. A hosted source must first be
ingested into that managed row; it must not bypass the contract by writing a native-keyed Redis stream.
"""
from __future__ import annotations


def main() -> None:
    raise SystemExit(
        "vexa_cloud_bridge.py was retired: ingest hosted transcripts into a managed numeric meeting "
        "row, then use the authenticated /api/meeting/process flow"
    )


if __name__ == "__main__":
    main()
