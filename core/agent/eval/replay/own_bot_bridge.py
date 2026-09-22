#!/usr/bin/env python3
"""Retired native-keyed meeting bridge.

The old replay copied a shared transcript into ``tc:meeting:{native}`` and called the retired
``POST /api/meeting/start`` route. Native meeting links collide across tenants and are not connected
to the numeric retention fence, so keeping that path runnable would preserve a consent and isolation
bypass in an "eval" utility.

Use the normal managed flow instead: create the bot through the gateway, keep the collector/watcher on
the numeric meeting row, and enable the owner-scoped ``POST /api/meeting/process`` toggle from an
authenticated client. That exercises the same path shipped to production.
"""
from __future__ import annotations


def main() -> None:
    raise SystemExit(
        "own_bot_bridge.py was retired: use managed POST /bots followed by the authenticated "
        "numeric-row /api/meeting/process flow"
    )


if __name__ == "__main__":
    main()
