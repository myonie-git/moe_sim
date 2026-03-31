"""Packet representation."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Packet:
    """Packet-level communication unit."""

    packet_id: str
    source_tile_id: int
    destination_tile_id: int
    size: int
    injection_time: int
    payload_tag: str
    collective_op_id: str
    group_id: str
    network_entry_time: int | None = None
    delivery_time: int | None = None
    hop_count: int = 0
    path_taken: list[int] = field(default_factory=list)
    planned_path: list[int] = field(default_factory=list)
    queuing_delay: int = 0
