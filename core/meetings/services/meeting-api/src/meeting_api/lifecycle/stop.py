"""The user-stop path — DELETE /bots → request the bot leave, classify its exit (P3b).

Port of the parent ``meetings.stop_bot`` control-plane behaviour, reduced to the FSM seam:

  * **request_stop(record, publisher)** — mark the record's ``stop_requested`` (the parent's
    ``meeting.data["stop_requested"]`` — the canonical user-intent signal the exit classifier reads
    first) and PUBLISH a ``bot_commands:meeting:{meeting_id}`` ``{"action":"leave"}`` command (the
    parent's leave-command publish). The bot, hearing it, leaves and emits a terminal lifecycle
    event; that event is then classified (below) WITH a reason — never a silent jump to terminal.

  * **classify_user_stop(record)** — the parent's Pack-C/Pack-J rule, reduced: a user stop is NEVER
    a failure regardless of stage. While the meeting was still ``joining``/``awaiting_admission`` the
    bot is asked to leave; its terminal exit lands as ``completed(stopped)`` — a TERMINAL state with
    the ``stopped`` reason, attributed to ``transition_source=user_stop`` — not a silent jump. The
    bot itself can only REPORT ``failed`` before ``active`` (its sealed FSM has no other terminal
    there); ``LifecycleSink.apply_change`` applies this classification to that report off the
    record's ``stop_requested`` and keeps the stage reached in ``failure_stage`` (L-0165, #807).

The leave command + the ``stop_requested`` flag are the two effects the eval asserts. The publisher
is a port (``LeaveCommandPublisher``) so the eval drives it with fakeredis / an in-memory capture.
"""
from __future__ import annotations

import json
from typing import Any, Optional, Protocol, runtime_checkable

from .machine import (
    BotStatus,
    CompletionReason,
    MeetingRecord,
    TransitionSource,
)


def leave_command_channel(meeting_id: Any) -> str:
    """The redis pub/sub channel the bot listens on for commands (parent's exact key)."""
    return f"bot_commands:meeting:{meeting_id}"


def leave_command_payload(meeting_id: Any) -> dict:
    """The leave-command body the parent publishes: ``{"action":"leave","meeting_id":id}``."""
    return {"action": "leave", "meeting_id": meeting_id}


@runtime_checkable
class LeaveCommandPublisher(Protocol):
    """The redis side of the stop path: publish a JSON command to a channel.

    Mirrors ``redis_client.publish(channel, json.dumps(payload))``. ``fakeredis.aioredis`` satisfies
    this directly; the eval can also use an in-memory capture.
    """

    async def publish(self, channel: str, message: str) -> Any:
        ...


async def request_stop(
    record: MeetingRecord,
    publisher: LeaveCommandPublisher,
    *,
    meeting_id: Any,
) -> dict:
    """Mark the record stop-requested and publish the ``leave`` command. Returns the command.

    Idempotent on the flag (a second stop just re-publishes — the parent does too). This does NOT
    flip the FSM to terminal: the bot's own terminal lifecycle event does that (then classified).
    """
    record.stop_requested = True
    channel = leave_command_channel(meeting_id)
    payload = leave_command_payload(meeting_id)
    await publisher.publish(channel, json.dumps(payload))
    return {"channel": channel, "payload": payload}


def classify_user_stop(record: MeetingRecord) -> tuple[BotStatus, CompletionReason]:
    """The reduced Pack-C/Pack-J rule: a user stop terminates ``completed`` WITH the ``stopped`` reason.

    The parent's ``_classify_stopped_exit`` short-circuits on ``stop_requested`` → a user DELETE is
    never a failure regardless of stage; the reason is always ``stopped``. This holds BEFORE
    ``active`` too (L-0165): a user who cancels while the bot is still ``joining`` /
    ``awaiting_admission`` cancelled — nothing failed — and recording that ``failed`` counted every
    lobby cancellation as a capture failure (5 of the 10 failures behind L-0020 were this).

    The bot's DOMAIN FSM (lifecycle.v1) only reaches ``completed`` from ``active``, so before
    ``active`` the bot can only REPORT ``failed``; the classification is the server's.
    ``LifecycleSink.apply_change`` applies this rule to that report off the record's
    ``stop_requested`` (the terminal edge is legal after a requested stop) and keeps the stage the
    bot reached in ``failure_stage`` — so ``completed`` + ``failure_stage`` reads "stopped before it
    got in", never mistaken for a run that delivered a meeting (#807: the stage survives the stop).
    """
    return (BotStatus.COMPLETED, CompletionReason.STOPPED)


def stop_event_for(record: MeetingRecord, *, exit_code: int = 0) -> dict:
    """Build the terminal lifecycle.v1 event a user stop resolves to, as the server classifies it.

    ``completed`` at every stage, carrying ``completion_reason=stopped`` — the attributable terminal
    a user stop resolves to. The caller feeds this through
    ``LifecycleSink.apply_change(..., transition_source=user_stop)`` on a record whose
    ``stop_requested`` was set by ``request_stop`` (which is what makes the pre-active
    ``completed`` edge legal there, and what stamps the stage reached).
    """
    status, reason = classify_user_stop(record)
    return {
        "connection_id": record.connection_id,
        "status": status.value,
        "exit_code": exit_code,
        "completion_reason": reason.value,
    }


USER_STOP = TransitionSource.USER_STOP
