"""Event queue primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    """Simulation event types."""

    PACKET_INJECT = "PACKET_INJECT"
    PACKET_ARRIVE_AT_ROUTER = "PACKET_ARRIVE_AT_ROUTER"
    PACKET_TRAVERSE_LINK = "PACKET_TRAVERSE_LINK"
    PACKET_DELIVER = "PACKET_DELIVER"
    CCL_STEP_COMPLETE = "CCL_STEP_COMPLETE"
    COLLECTIVE_OP_COMPLETE = "COLLECTIVE_OP_COMPLETE"
    WORKLOAD_STEP_TRIGGER = "WORKLOAD_STEP_TRIGGER"


@dataclass(order=True)
class Event:
    """Priority-queue item for the simulator."""

    timestamp: int
    sequence: int
    event_type: EventType = field(compare=False)
    payload: dict[str, Any] = field(compare=False, default_factory=dict)
