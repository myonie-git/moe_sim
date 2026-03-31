"""Tile model for wafer-scale topologies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Tile:
    """A single processing tile/core in the topology."""

    tile_id: int
    x: int
    y: int
    injection_bandwidth: int
    ejection_bandwidth: int
    local_buffer_size: int
    compute_capacity: int
    injection_queue_capacity: int
    ejection_queue_capacity: int
    defective: bool = False
    routing_state: dict[str, Any] = field(default_factory=dict)
    injection_available_at: int = 0
    ejection_available_at: int = 0
    collectives_participated: set[str] = field(default_factory=set)
    active_cycles: int = 0
    idle_cycles: int = 0
    injection_stall_cycles: int = 0
    ejection_stall_cycles: int = 0

    def reset_runtime(self) -> None:
        """Reset mutable runtime state before a simulation run."""

        self.injection_available_at = 0
        self.ejection_available_at = 0
        self.collectives_participated.clear()
        self.active_cycles = 0
        self.idle_cycles = 0
        self.injection_stall_cycles = 0
        self.ejection_stall_cycles = 0
