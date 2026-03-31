import pytest

from wafer_sim import Simulator
from wafer_sim.ccl.mapping import RankMapping
from wafer_sim.workload.comm_group import CommGroup, GroupBuilder


def test_row_groups_cover_each_row() -> None:
    simulator = Simulator(topology_type="mesh", width=4, height=2)
    groups = GroupBuilder().row_groups(simulator.topology)
    assert [group.group_id for group in groups] == ["row_0", "row_1"]
    assert groups[0].tile_ids == [0, 1, 2, 3]
    assert groups[1].tile_ids == [4, 5, 6, 7]


def test_duplicate_tile_ids_are_rejected() -> None:
    simulator = Simulator(topology_type="mesh", width=2, height=2)
    group = CommGroup(group_id="dup", tile_ids=[0, 0])
    with pytest.raises(ValueError):
        group.validate(simulator.topology)


def test_snake_rank_mapping_reorders_tiles() -> None:
    simulator = Simulator(topology_type="mesh", width=3, height=2)
    group = CommGroup(
        group_id="snake",
        tile_ids=[0, 1, 2, 3, 4, 5],
        rank_mapping=RankMapping.SNAKE,
    )
    assert group.ordered_tile_ids(simulator.topology) == [0, 1, 2, 5, 4, 3]
