"""Directional link model."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Link:
    """A directional inter-tile link."""

    src_tile_id: int
    dst_tile_id: int
    bandwidth: int
    latency: int
    full_duplex: bool
    buffer_size: int
    defective: bool = False
    available_at: int = 0
    total_bytes_transferred: int = 0
    busy_cycles: int = 0
    peak_queue_depth: int = 0
    bytes_by_collective: dict[str, int] = field(default_factory=dict)

    def reset_runtime(self) -> None:
        """Reset mutable runtime state."""

        self.available_at = 0
        self.total_bytes_transferred = 0
        self.busy_cycles = 0
        self.peak_queue_depth = 0
        self.bytes_by_collective.clear()
