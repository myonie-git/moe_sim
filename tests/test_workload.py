from pathlib import Path

from wafer_sim import Simulator
from wafer_sim.workload.yaml_parser import load_workload_from_yaml


def test_workload_yaml_expands_group_patterns_and_dependencies(tmp_path: Path) -> None:
    workload_path = tmp_path / "workload.yaml"
    workload_path.write_text(
        """
workload:
  name: "rows_demo"
  groups:
    - group_id: "row_{i}"
      type: "row"
  ops:
    - op_id: "gather_{i}"
      op_type: "allgather"
      group_pattern: "row_*"
      data_size: 64
      algorithm: "ring_allgather"
    - op_id: "scatter_{i}"
      op_type: "reduce_scatter"
      group_pattern: "row_*"
      data_size: 64
      algorithm: "ring_reduce_scatter"
      depends_on: ["gather_*"]
""".strip(),
        encoding="utf-8",
    )
    simulator = Simulator(topology_type="mesh", width=4, height=2)
    workload = load_workload_from_yaml(workload_path, simulator.topology)
    assert sorted(workload.groups) == ["row_0", "row_1"]
    assert [op.op_id for op in workload.ops] == [
        "gather_0",
        "gather_1",
        "scatter_0",
        "scatter_1",
    ]
    assert workload.ops[2].depends_on == ["gather_0", "gather_1"]


def test_workload_executes_dependency_chain(tmp_path: Path) -> None:
    workload_path = tmp_path / "workload.yaml"
    workload_path.write_text(
        """
workload:
  name: "chain"
  groups:
    - group_id: "row_{i}"
      type: "row"
  ops:
    - op_id: "gather_{i}"
      op_type: "allgather"
      group_pattern: "row_*"
      data_size: 64
      algorithm: "ring_allgather"
    - op_id: "allreduce_{i}"
      op_type: "allreduce"
      group_pattern: "row_*"
      data_size: 64
      algorithm: "ring_allreduce"
      depends_on: ["gather_*"]
""".strip(),
        encoding="utf-8",
    )
    simulator = Simulator(topology_type="mesh", width=4, height=2)
    results = simulator.run_workload_from_yaml(workload_path)
    assert results.total_completion_time > 0
    assert len(results.op_results) == 4
    assert any(op.op_id.startswith("allreduce_") for op in results.op_results)
