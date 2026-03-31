import pytest

from wafer_sim import Workload
from wafer_sim.workload.collective_op import CollectiveOp
from wafer_sim.workload.comm_group import CommGroup


def test_workload_validation_rejects_cycles() -> None:
    workload = Workload(name="cycle")
    workload.add_group(CommGroup(group_id="g0", tile_ids=[0, 1]))
    workload.add_op(
        CollectiveOp(
            op_id="op_a",
            op_type="allgather",
            group_id="g0",
            data_size=64,
            algorithm="ring_allgather",
            depends_on=["op_b"],
        )
    )
    workload.add_op(
        CollectiveOp(
            op_id="op_b",
            op_type="allgather",
            group_id="g0",
            data_size=64,
            algorithm="ring_allgather",
            depends_on=["op_a"],
        )
    )
    with pytest.raises(ValueError):
        workload.validate()
