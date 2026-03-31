"""Base classes for collective algorithms."""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar


@dataclass
class Transfer:
    """One logical transfer inside a CCL step."""

    src_rank: int
    dst_rank: int
    chunk_id: int
    size: int
    reduce_op: str | None = None


@dataclass
class CCLStep:
    """A parallel communication step."""

    step_id: int
    transfers: list[Transfer]
    depends_on: list[int] = field(default_factory=list)
    compute_before: int = 0


class CCLAlgorithm(ABC):
    """Abstract base class for collective algorithms."""

    SUPPORTED_OPS: ClassVar[list[str]] = []

    def __init__(
        self,
        comm_group,
        data_size: int,
        num_chunks: int,
        simulator,
        **kwargs,
    ) -> None:
        self.comm_group = comm_group
        self.data_size = data_size
        self.num_chunks = max(1, num_chunks)
        self.simulator = simulator
        self.kwargs = kwargs

    @abstractmethod
    def generate_schedule(self) -> list[CCLStep]:
        """Return the algorithm's step schedule."""

    def get_supported_operations(self) -> list[str]:
        """Return supported collective op names."""

        return list(self.SUPPORTED_OPS)

    def on_step_complete(self, step_id: int) -> None:
        """Called when a step completes."""

    def on_complete(self) -> None:
        """Called when the collective completes."""

    def validate(self) -> bool:
        """Validate the algorithm against the current group."""

        return len(self.comm_group.tile_ids) >= 2

    def estimate_theoretical_lower_bound(self, schedule: list[CCLStep]) -> int:
        """Very simple alpha-beta style lower bound."""

        bytes_by_rank: dict[int, int] = {}
        for step in schedule:
            for transfer in step.transfers:
                bytes_by_rank[transfer.src_rank] = bytes_by_rank.get(transfer.src_rank, 0) + transfer.size
        max_bytes_per_rank = max(bytes_by_rank.values(), default=0)
        bottleneck = min(
            self.simulator.config.tile.injection_bandwidth,
            self.simulator.config.tile.ejection_bandwidth,
            self.simulator.config.link.bandwidth,
        )
        return math.ceil(max_bytes_per_rank / bottleneck) + len(schedule) * self.simulator.config.link.latency

    def estimate_bus_bandwidth(self, schedule: list[CCLStep], completion_time: int) -> float:
        """Bus-level throughput based on actual transfer bytes."""

        if completion_time <= 0:
            return 0.0
        total_bytes = sum(transfer.size for step in schedule for transfer in step.transfers)
        return float(total_bytes) / completion_time

    def _chunk_size(self, divisor: int) -> int:
        return math.ceil(self.data_size / max(1, divisor))
