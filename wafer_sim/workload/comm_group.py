"""Communication groups and builders."""

from __future__ import annotations

from dataclasses import dataclass, field
from math import ceil, isqrt

from wafer_sim.ccl.mapping import RankMapping
from wafer_sim.core.topology import Topology


def _hilbert_index(x: int, y: int, bits: int) -> int:
    index = 0
    n = 1 << bits
    tx = x
    ty = y
    scale = n // 2
    while scale > 0:
        rx = 1 if tx & scale else 0
        ry = 1 if ty & scale else 0
        index += scale * scale * ((3 * rx) ^ ry)
        if ry == 0:
            if rx == 1:
                tx = n - 1 - tx
                ty = n - 1 - ty
            tx, ty = ty, tx
        scale //= 2
    return index


@dataclass
class CommGroup:
    """A set of tiles participating in a collective."""

    group_id: str
    tile_ids: list[int]
    logical_rank_order: list[int] | None = None
    rank_mapping: RankMapping = RankMapping.NATURAL

    def validate(self, topology: Topology) -> None:
        """Validate tile ids, defects, and rank ordering."""

        if len(set(self.tile_ids)) != len(self.tile_ids):
            raise ValueError(f"Group '{self.group_id}' contains duplicate tile ids.")
        for tile_id in self.tile_ids:
            if not topology.is_tile_alive(tile_id):
                raise ValueError(
                    f"Group '{self.group_id}' references invalid or defective tile {tile_id}."
                )
        if self.logical_rank_order is not None:
            expected = list(range(len(self.tile_ids)))
            if sorted(self.logical_rank_order) != expected:
                raise ValueError(
                    f"Group '{self.group_id}' logical_rank_order must be a permutation of {expected}."
                )

    def ordered_tile_ids(self, topology: Topology | None = None) -> list[int]:
        """Return tile ids in logical rank order."""

        if self.logical_rank_order is not None:
            return [self.tile_ids[index] for index in self.logical_rank_order]
        if self.rank_mapping == RankMapping.NATURAL:
            return list(self.tile_ids)
        if topology is None:
            raise ValueError("A topology is required for non-natural rank mappings.")
        tiles = list(self.tile_ids)
        if self.rank_mapping == RankMapping.ROW_MAJOR:
            return sorted(tiles, key=lambda tile_id: _yx_key(tile_id, topology))
        if self.rank_mapping == RankMapping.COLUMN_MAJOR:
            return sorted(tiles, key=lambda tile_id: _xy_key(tile_id, topology))
        if self.rank_mapping == RankMapping.SNAKE:
            return sorted(
                tiles,
                key=lambda tile_id: _snake_key(tile_id, topology),
            )
        if self.rank_mapping == RankMapping.HILBERT:
            max_dim = max(topology.width, topology.height)
            bits = max(1, ceil(max_dim - 1).bit_length())
            return sorted(
                tiles,
                key=lambda tile_id: _hilbert_key(tile_id, topology, bits),
            )
        return list(self.tile_ids)


def _yx_key(tile_id: int, topology: Topology) -> tuple[int, int]:
    coord = topology.id_to_coordinate(tile_id)
    return coord.y, coord.x


def _xy_key(tile_id: int, topology: Topology) -> tuple[int, int]:
    coord = topology.id_to_coordinate(tile_id)
    return coord.x, coord.y


def _snake_key(tile_id: int, topology: Topology) -> tuple[int, int]:
    coord = topology.id_to_coordinate(tile_id)
    if coord.y % 2 == 0:
        return coord.y, coord.x
    return coord.y, topology.width - 1 - coord.x


def _hilbert_key(tile_id: int, topology: Topology, bits: int) -> int:
    coord = topology.id_to_coordinate(tile_id)
    return _hilbert_index(coord.x, coord.y, bits)


class GroupBuilder:
    """Factory methods for common communication groups."""

    def row_groups(self, topology: Topology) -> list[CommGroup]:
        groups = []
        for y in range(topology.height):
            tile_ids = [
                topology.coordinate_to_id(x, y)
                for x in range(topology.width)
                if topology.is_tile_alive(topology.coordinate_to_id(x, y))
            ]
            if tile_ids:
                groups.append(CommGroup(group_id=f"row_{y}", tile_ids=tile_ids))
        return groups

    def column_groups(self, topology: Topology) -> list[CommGroup]:
        groups = []
        for x in range(topology.width):
            tile_ids = [
                topology.coordinate_to_id(x, y)
                for y in range(topology.height)
                if topology.is_tile_alive(topology.coordinate_to_id(x, y))
            ]
            if tile_ids:
                groups.append(CommGroup(group_id=f"column_{x}", tile_ids=tile_ids))
        return groups

    def rectangular_block(
        self,
        topology: Topology,
        x0: int,
        y0: int,
        x1: int,
        y1: int,
        group_id: str | None = None,
    ) -> CommGroup:
        tile_ids = []
        for y in range(y0, y1 + 1):
            for x in range(x0, x1 + 1):
                tile_id = topology.coordinate_to_id(x, y)
                if topology.is_tile_alive(tile_id):
                    tile_ids.append(tile_id)
        return CommGroup(group_id=group_id or f"rect_{x0}_{y0}_{x1}_{y1}", tile_ids=tile_ids)

    def every_n_tiles(
        self, topology: Topology, n: int, strategy: str = "row_major"
    ) -> list[CommGroup]:
        if n <= 0:
            raise ValueError("n must be positive.")
        strategy = strategy.lower()
        if strategy == "block":
            return self._block_groups(topology, n)
        tile_ids = topology.active_tile_ids()
        if strategy == "column_major":
            tile_ids = sorted(tile_ids, key=lambda tile_id: _xy_key(tile_id, topology))
        elif strategy == "interleaved":
            group_count = ceil(len(tile_ids) / n)
            buckets = [[] for _ in range(group_count)]
            for index, tile_id in enumerate(sorted(tile_ids, key=lambda item: _yx_key(item, topology))):
                buckets[index % group_count].append(tile_id)
            return [
                CommGroup(group_id=f"interleaved_{index}", tile_ids=tiles)
                for index, tiles in enumerate(buckets)
                if tiles
            ]
        else:
            tile_ids = sorted(tile_ids, key=lambda tile_id: _yx_key(tile_id, topology))
        groups = []
        for start in range(0, len(tile_ids), n):
            groups.append(CommGroup(group_id=f"group_{start // n}", tile_ids=tile_ids[start : start + n]))
        return groups

    def strided_group(self, topology: Topology, start: int, stride: int, count: int) -> CommGroup:
        tile_ids = [start + index * stride for index in range(count)]
        group = CommGroup(group_id=f"strided_{start}_{stride}_{count}", tile_ids=tile_ids)
        group.validate(topology)
        return group

    def custom(self, group_id: str, tile_ids: list[int]) -> CommGroup:
        return CommGroup(group_id=group_id, tile_ids=list(tile_ids))

    def from_spec(self, spec: dict[str, object], topology: Topology) -> list[CommGroup]:
        group_type = str(spec["type"]).lower()
        group_id_template = str(spec.get("group_id", "group_{i}"))
        if group_type == "row":
            groups = self.row_groups(topology)
            return self._rename_groups(groups, group_id_template)
        if group_type == "column":
            groups = self.column_groups(topology)
            return self._rename_groups(groups, group_id_template)
        if group_type == "rectangular_block":
            group = self.rectangular_block(
                topology,
                int(spec["x0"]),
                int(spec["y0"]),
                int(spec["x1"]),
                int(spec["y1"]),
                group_id=group_id_template.replace("{i}", "0"),
            )
            if "rank_mapping" in spec:
                group.rank_mapping = RankMapping(str(spec["rank_mapping"]))
            group.validate(topology)
            return [group]
        if group_type == "every_n":
            groups = self.every_n_tiles(
                topology,
                n=int(spec["n"]),
                strategy=str(spec.get("strategy", "row_major")),
            )
            groups = self._rename_groups(groups, group_id_template)
            for group in groups:
                if "rank_mapping" in spec:
                    group.rank_mapping = RankMapping(str(spec["rank_mapping"]))
                group.validate(topology)
            return groups
        if group_type == "strided":
            group = self.strided_group(
                topology,
                start=int(spec["start"]),
                stride=int(spec["stride"]),
                count=int(spec["count"]),
            )
            group.group_id = group_id_template.replace("{i}", "0")
            return [group]
        if group_type == "custom":
            group = self.custom(
                group_id=group_id_template.replace("{i}", "0"),
                tile_ids=[int(tile_id) for tile_id in spec["tile_ids"]],
            )
            if "rank_mapping" in spec:
                group.rank_mapping = RankMapping(str(spec["rank_mapping"]))
            if "logical_rank_order" in spec:
                group.logical_rank_order = [int(index) for index in spec["logical_rank_order"]]
            group.validate(topology)
            return [group]
        raise ValueError(f"Unsupported group type '{group_type}'.")

    @staticmethod
    def _rename_groups(groups: list[CommGroup], template: str) -> list[CommGroup]:
        renamed = []
        for index, group in enumerate(groups):
            group.group_id = template.replace("{i}", str(index))
            renamed.append(group)
        return renamed

    def _block_groups(self, topology: Topology, n: int) -> list[CommGroup]:
        width, height = self._block_dimensions(n)
        groups = []
        group_index = 0
        for y in range(0, topology.height, height):
            for x in range(0, topology.width, width):
                tile_ids = []
                for inner_y in range(y, min(y + height, topology.height)):
                    for inner_x in range(x, min(x + width, topology.width)):
                        tile_id = topology.coordinate_to_id(inner_x, inner_y)
                        if topology.is_tile_alive(tile_id):
                            tile_ids.append(tile_id)
                if tile_ids:
                    groups.append(CommGroup(group_id=f"block_{group_index}", tile_ids=tile_ids))
                    group_index += 1
        return groups

    @staticmethod
    def _block_dimensions(n: int) -> tuple[int, int]:
        best_width, best_height = n, 1
        best_delta = n
        for factor in range(1, isqrt(n) + 1):
            if n % factor != 0:
                continue
            other = n // factor
            if abs(other - factor) < best_delta:
                best_width, best_height = other, factor
                best_delta = abs(other - factor)
        return best_width, best_height
