from wafer_sim import Simulator
from wafer_sim.ccl import CCLAlgorithmRegistry
from wafer_sim.workload.comm_group import CommGroup


def test_algorithm_registry_contains_reference_algorithms() -> None:
    algorithms = CCLAlgorithmRegistry.list_algorithms()
    assert "ring_allreduce" in algorithms
    assert "ring_allgather" in algorithms
    assert "ring_reduce_scatter" in algorithms
    assert "naive_all_to_all" in algorithms


def test_run_single_collective_returns_metrics() -> None:
    simulator = Simulator(topology_type="mesh", width=4, height=1)
    group = CommGroup(group_id="line", tile_ids=[0, 1, 2, 3])
    result = simulator.run_single_collective(
        group=group,
        op_type="allreduce",
        data_size=128,
        algorithm="ring_allreduce",
        algorithm_params={"num_chunks": 2},
    )
    assert result.op_id == "single_op"
    assert result.algorithm == "ring_allreduce"
    assert result.completion_time > 0
    assert result.algorithm_bandwidth > 0
