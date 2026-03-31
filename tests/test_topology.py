from wafer_sim import Simulator


def test_mesh_neighbors_and_tile_ids() -> None:
    simulator = Simulator(topology_type="mesh", width=3, height=3)
    topology = simulator.topology
    center = topology.coordinate_to_id(1, 1)
    expected = {
        topology.coordinate_to_id(1, 0),
        topology.coordinate_to_id(2, 1),
        topology.coordinate_to_id(1, 2),
        topology.coordinate_to_id(0, 1),
    }
    assert set(topology.neighbors(center)) == expected
    assert topology.coordinate_to_id(2, 2) == 8


def test_defects_remove_tiles_and_links() -> None:
    simulator = Simulator(
        topology_type="mesh",
        width=3,
        height=3,
        defects={
            "dead_tiles": [[1, 1]],
            "dead_links": [[[0, 0], [1, 0]]],
        },
    )
    topology = simulator.topology
    assert not topology.is_tile_alive(topology.coordinate_to_id(1, 1))
    assert topology.get_link(
        topology.coordinate_to_id(0, 0),
        topology.coordinate_to_id(1, 0),
    ) is None
