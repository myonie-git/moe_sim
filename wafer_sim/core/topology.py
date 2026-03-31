"""Topology graph construction."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from wafer_sim.config.loader import DefectsConfig, LinkConfig, ReticleConfig, TileConfig
from wafer_sim.core.link import Link
from wafer_sim.core.tile import Tile

_DIRECTION_DELTAS: dict[str, tuple[int, int]] = {
    "north": (0, -1),
    "south": (0, 1),
    "west": (-1, 0),
    "east": (1, 0),
}


@dataclass(frozen=True)
class Coordinate:
    """Immutable tile coordinate."""

    x: int
    y: int


class Topology:
    """2D mesh or torus represented as an adjacency list."""

    def __init__(
        self,
        topology_type: str,
        width: int,
        height: int,
        tile_config: TileConfig,
        link_config: LinkConfig,
        reticle_config: ReticleConfig | None = None,
        defects_config: DefectsConfig | None = None,
    ) -> None:
        self.topology_type = topology_type.lower()
        self.width = width
        self.height = height
        self.tile_config = tile_config
        self.link_config = link_config
        self.reticle_config = reticle_config or ReticleConfig()
        self.defects_config = defects_config or DefectsConfig()
        self.tiles: dict[int, Tile] = {}
        self.links: dict[tuple[int, int], Link] = {}
        self.adjacency: dict[int, list[int]] = defaultdict(list)
        self.dead_tile_ids = {
            self.coordinate_to_id(x, y) for x, y in self.defects_config.dead_tiles
        }
        self.dead_links: set[tuple[int, int]] = set()
        for endpoints in self.defects_config.dead_links:
            (x1, y1), (x2, y2) = endpoints
            src = self.coordinate_to_id(x1, y1)
            dst = self.coordinate_to_id(x2, y2)
            self.dead_links.add((src, dst))
            self.dead_links.add((dst, src))
        self._build_tiles()
        self._build_links()

    def coordinate_to_id(self, x: int, y: int) -> int:
        """Translate coordinates into a flat tile id."""

        if not (0 <= x < self.width and 0 <= y < self.height):
            raise ValueError(f"Coordinate ({x}, {y}) is outside the topology bounds.")
        return y * self.width + x

    def id_to_coordinate(self, tile_id: int) -> Coordinate:
        """Translate a flat tile id into coordinates."""

        if tile_id < 0 or tile_id >= self.width * self.height:
            raise ValueError(f"Tile id {tile_id} is outside the topology bounds.")
        return Coordinate(x=tile_id % self.width, y=tile_id // self.width)

    def active_tile_ids(self) -> list[int]:
        """Return all non-defective tile ids in deterministic order."""

        return [tile_id for tile_id in sorted(self.tiles) if not self.tiles[tile_id].defective]

    def is_tile_alive(self, tile_id: int) -> bool:
        """Whether the tile exists and is not marked defective."""

        return tile_id in self.tiles and not self.tiles[tile_id].defective

    def neighbors(self, tile_id: int) -> list[int]:
        """Return live adjacent tiles for the given tile."""

        return list(self.adjacency.get(tile_id, []))

    def neighbor_in_direction(self, tile_id: int, direction: str) -> int | None:
        """Return the live neighbor in a cardinal direction, if one exists."""

        coord = self.id_to_coordinate(tile_id)
        dx, dy = _DIRECTION_DELTAS[direction]
        next_x = coord.x + dx
        next_y = coord.y + dy
        if self.topology_type == "torus":
            next_x %= self.width
            next_y %= self.height
        if not (0 <= next_x < self.width and 0 <= next_y < self.height):
            return None
        neighbor = self.coordinate_to_id(next_x, next_y)
        if not self.is_tile_alive(neighbor):
            return None
        if (tile_id, neighbor) in self.dead_links:
            return None
        return neighbor

    def direction_between(self, src_tile_id: int, dst_tile_id: int) -> str | None:
        """Return the cardinal direction from src to dst if they are adjacent."""

        src = self.id_to_coordinate(src_tile_id)
        dst = self.id_to_coordinate(dst_tile_id)
        for direction, (dx, dy) in _DIRECTION_DELTAS.items():
            nx = src.x + dx
            ny = src.y + dy
            if self.topology_type == "torus":
                nx %= self.width
                ny %= self.height
            if nx == dst.x and ny == dst.y:
                return direction
        return None

    def get_link(self, src_tile_id: int, dst_tile_id: int) -> Link | None:
        """Return the directed link from src to dst if it exists."""

        return self.links.get((src_tile_id, dst_tile_id))

    def reset_runtime(self) -> None:
        """Reset runtime state before a new simulation run."""

        for tile in self.tiles.values():
            tile.reset_runtime()
        for link in self.links.values():
            link.reset_runtime()

    def _build_tiles(self) -> None:
        for y in range(self.height):
            for x in range(self.width):
                tile_id = self.coordinate_to_id(x, y)
                self.tiles[tile_id] = Tile(
                    tile_id=tile_id,
                    x=x,
                    y=y,
                    injection_bandwidth=self.tile_config.injection_bandwidth,
                    ejection_bandwidth=self.tile_config.ejection_bandwidth,
                    local_buffer_size=self.tile_config.local_buffer_size,
                    compute_capacity=self.tile_config.compute_capacity,
                    injection_queue_capacity=self.tile_config.injection_queue_capacity,
                    ejection_queue_capacity=self.tile_config.ejection_queue_capacity,
                    defective=tile_id in self.dead_tile_ids,
                )

    def _build_links(self) -> None:
        for tile_id, tile in self.tiles.items():
            if tile.defective:
                continue
            for direction in _DIRECTION_DELTAS:
                neighbor = self.neighbor_in_direction(tile_id, direction)
                if neighbor is None:
                    continue
                if self.tiles[neighbor].defective or (tile_id, neighbor) in self.dead_links:
                    continue
                bandwidth, latency = self._link_properties(tile_id, neighbor)
                self.links[(tile_id, neighbor)] = Link(
                    src_tile_id=tile_id,
                    dst_tile_id=neighbor,
                    bandwidth=bandwidth,
                    latency=latency,
                    full_duplex=self.link_config.full_duplex,
                    buffer_size=self.link_config.buffer_size,
                    defective=False,
                )
                self.adjacency[tile_id].append(neighbor)

    def _link_properties(self, src_tile_id: int, dst_tile_id: int) -> tuple[int, int]:
        if self._crosses_reticle_boundary(src_tile_id, dst_tile_id):
            bandwidth = (
                self.reticle_config.cross_reticle_bandwidth or self.link_config.bandwidth
            )
            latency = self.reticle_config.cross_reticle_latency or self.link_config.latency
            return bandwidth, latency
        return self.link_config.bandwidth, self.link_config.latency

    def _crosses_reticle_boundary(self, src_tile_id: int, dst_tile_id: int) -> bool:
        if not self.reticle_config.enabled:
            return False
        period = max(1, self.reticle_config.boundary_period)
        src = self.id_to_coordinate(src_tile_id)
        dst = self.id_to_coordinate(dst_tile_id)
        return (src.x // period != dst.x // period) or (src.y // period != dst.y // period)
