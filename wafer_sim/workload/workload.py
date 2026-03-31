"""Workload container and validation."""

from __future__ import annotations

from dataclasses import dataclass

from wafer_sim.ccl.registry import CCLAlgorithmRegistry
from wafer_sim.workload.collective_op import CollectiveOp
from wafer_sim.workload.comm_group import CommGroup


class Workload:
    """A validated list of groups and collective operations."""

    def __init__(self, name: str):
        self.name = name
        self.groups: dict[str, CommGroup] = {}
        self.ops: list[CollectiveOp] = []

    def add_group(self, group: CommGroup) -> None:
        """Register a communication group."""

        if group.group_id in self.groups:
            raise ValueError(f"Duplicate group_id '{group.group_id}'.")
        self.groups[group.group_id] = group

    def add_op(self, op: CollectiveOp) -> None:
        """Add a collective op."""

        if any(existing.op_id == op.op_id for existing in self.ops):
            raise ValueError(f"Duplicate op_id '{op.op_id}'.")
        self.ops.append(op)

    def validate(self, topology=None) -> None:
        """Validate groups, algorithms, and op dependency DAG."""

        if topology is not None:
            for group in self.groups.values():
                group.validate(topology)
        op_ids = {op.op_id for op in self.ops}
        if len(op_ids) != len(self.ops):
            raise ValueError("Operation ids must be unique.")
        for op in self.ops:
            if op.group_id not in self.groups:
                raise ValueError(f"Op '{op.op_id}' references unknown group '{op.group_id}'.")
            algorithm_class = CCLAlgorithmRegistry.get_algorithm_class(op.algorithm)
            supported_ops = getattr(algorithm_class, "SUPPORTED_OPS", [])
            if op.op_type not in supported_ops:
                raise ValueError(
                    f"Algorithm '{op.algorithm}' does not support op type '{op.op_type}'."
                )
            for dependency in op.depends_on:
                if dependency not in op_ids:
                    raise ValueError(
                        f"Op '{op.op_id}' depends on unknown op '{dependency}'."
                    )
        self._validate_dag()

    def _validate_dag(self) -> None:
        visiting: set[str] = set()
        visited: set[str] = set()
        by_id = {op.op_id: op for op in self.ops}

        def dfs(op_id: str) -> None:
            if op_id in visited:
                return
            if op_id in visiting:
                raise ValueError("Workload dependencies must form a DAG.")
            visiting.add(op_id)
            for dependency in by_id[op_id].depends_on:
                dfs(dependency)
            visiting.remove(op_id)
            visited.add(op_id)

        for op_id in by_id:
            dfs(op_id)
