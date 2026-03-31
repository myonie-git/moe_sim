"""Naive All-to-All reference implementation."""

from __future__ import annotations

from wafer_sim.ccl.base import CCLAlgorithm, CCLStep, Transfer
from wafer_sim.ccl.registry import CCLAlgorithmRegistry


@CCLAlgorithmRegistry.register("naive_all_to_all")
class NaiveAllToAll(CCLAlgorithm):
    """Naive pairwise All-to-All.

    Each rank sends a peer-specific slice to every other rank in turn.
    Time complexity is O((N - 1) * num_chunks) rounds.
    Ideal bandwidth is approximately total_bytes_sent / completion_time.
    """

    SUPPORTED_OPS = ["all_to_all"]

    def generate_schedule(self) -> list[CCLStep]:
        rank_count = len(self.comm_group.tile_ids)
        if rank_count < 2:
            return []
        chunk_size = self._chunk_size((rank_count - 1) * self.num_chunks)
        steps: list[CCLStep] = []
        step_id = 0
        for peer_offset in range(1, rank_count):
            for micro_step in range(self.num_chunks):
                transfers = [
                    Transfer(
                        src_rank=rank,
                        dst_rank=(rank + peer_offset) % rank_count,
                        chunk_id=(peer_offset - 1) * self.num_chunks + micro_step,
                        size=chunk_size,
                    )
                    for rank in range(rank_count)
                ]
                steps.append(CCLStep(step_id=step_id, transfers=transfers))
                step_id += 1
        return steps
