"""YAML workload parsing with glob expansion."""

from __future__ import annotations

from fnmatch import fnmatch
from pathlib import Path

import yaml

from wafer_sim.core.topology import Topology
from wafer_sim.workload.collective_op import CollectiveOp
from wafer_sim.workload.comm_group import GroupBuilder
from wafer_sim.workload.workload import Workload


def load_workload_from_yaml(path: str | Path, topology: Topology) -> Workload:
    """Parse a workload YAML file and expand group/op globs."""

    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    root = raw.get("workload", raw)
    if not isinstance(root, dict):
        raise ValueError("Workload YAML must contain a mapping.")
    workload = Workload(name=str(root.get("name", "workload")))
    builder = GroupBuilder()
    for group_spec in root.get("groups", []):
        for group in builder.from_spec(group_spec, topology):
            workload.add_group(group)
    op_specs = root.get("ops", [])
    explicit_dependencies: dict[str, list[str]] = {}
    ordered_group_ids = sorted(workload.groups)
    for op_spec in op_specs:
        if "group_pattern" in op_spec:
            pattern = str(op_spec["group_pattern"])
            matches = [group_id for group_id in ordered_group_ids if fnmatch(group_id, pattern)]
            if not matches:
                raise ValueError(f"Group pattern '{pattern}' matched no groups.")
            for index, group_id in enumerate(matches):
                suffix = _group_suffix(group_id, index)
                op = _build_op(op_spec, group_id, suffix)
                workload.add_op(op)
                explicit_dependencies[op.op_id] = [str(item) for item in op_spec.get("depends_on", [])]
        else:
            group_id = str(op_spec["group_id"])
            op = _build_op(op_spec, group_id, "0")
            workload.add_op(op)
            explicit_dependencies[op.op_id] = [str(item) for item in op_spec.get("depends_on", [])]
    all_op_ids = [op.op_id for op in workload.ops]
    for op in workload.ops:
        op.depends_on = _expand_dependencies(explicit_dependencies[op.op_id], all_op_ids)
    workload.validate(topology=topology)
    return workload


def _build_op(op_spec: dict[str, object], group_id: str, suffix: str) -> CollectiveOp:
    op_id_template = str(op_spec["op_id"])
    op_id = op_id_template.replace("{i}", suffix).replace("{group_id}", group_id)
    return CollectiveOp(
        op_id=op_id,
        op_type=str(op_spec["op_type"]),
        group_id=group_id,
        data_size=int(op_spec["data_size"]),
        algorithm=str(op_spec["algorithm"]),
        algorithm_params=dict(op_spec.get("algorithm_params", {})),
        depends_on=[],
        start_time=int(op_spec["start_time"]) if "start_time" in op_spec else None,
    )


def _expand_dependencies(patterns: list[str], op_ids: list[str]) -> list[str]:
    expanded: list[str] = []
    for pattern in patterns:
        if any(char in pattern for char in "*?[]"):
            matches = [op_id for op_id in op_ids if fnmatch(op_id, pattern)]
            expanded.extend(matches)
        else:
            expanded.append(pattern)
    deduplicated = []
    seen = set()
    for dependency in expanded:
        if dependency not in seen:
            deduplicated.append(dependency)
            seen.add(dependency)
    return deduplicated


def _group_suffix(group_id: str, index: int) -> str:
    suffix = group_id.rsplit("_", maxsplit=1)[-1]
    return suffix if suffix != group_id else str(index)
