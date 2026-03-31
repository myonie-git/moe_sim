"""Programmatic single-op example."""

from wafer_sim import CommGroup, Simulator


def main() -> None:
    simulator = Simulator(topology_type="mesh", width=4, height=4)
    group = CommGroup(group_id="block0", tile_ids=[0, 1, 4, 5])
    result = simulator.run_single_collective(
        group=group,
        op_type="allreduce",
        data_size=1024,
        algorithm="ring_allreduce",
        algorithm_params={"num_chunks": 2},
    )
    print(result)


if __name__ == "__main__":
    main()
