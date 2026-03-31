"""Naive ring AllGather reference implementation."""

from __future__ import annotations

from wafer_sim.ccl.base import CCLAlgorithm, CCLStep, Transfer
from wafer_sim.ccl.registry import CCLAlgorithmRegistry


@CCLAlgorithmRegistry.register("ring_allgather")
class NaiveRingAllGather(CCLAlgorithm):
    """Simple ring AllGather.

    Each rank forwards one chunk per round to its clockwise neighbor.
    Time complexity is O((N - 1) * num_chunks).
    Expected ideal bandwidth is roughly total_bytes_sent / completion_time.
    """

    SUPPORTED_OPS = ["allgather"]

    def generate_schedule(self) -> list[CCLStep]:
        rank_count = len(self.comm_group.tile_ids)
        chunk_size = self._chunk_size(rank_count * self.num_chunks)
        steps: list[CCLStep] = []
        step_id = 0
        for round_idx in range(rank_count - 1):
            for micro_step in range(self.num_chunks):
                transfers = [
                    Transfer(
                        src_rank=rank,
                        dst_rank=(rank + 1) % rank_count,
                        chunk_id=round_idx * self.num_chunks + micro_step,
                        size=chunk_size,
                    )
                    for rank in range(rank_count)
                ]
                steps.append(CCLStep(step_id=step_id, transfers=transfers))
                step_id += 1
        return steps
