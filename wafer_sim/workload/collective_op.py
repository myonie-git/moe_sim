"""Collective operation descriptors."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CollectiveOp:
    """One collective operation inside a workload."""

    op_id: str
    op_type: str
    group_id: str
    data_size: int
    algorithm: str
    algorithm_params: dict[str, object] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    start_time: int | None = None
