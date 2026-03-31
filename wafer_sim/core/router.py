"""Routing functions and path computation."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Callable, Dict

from wafer_sim.core.topology import Coordinate, Topology

CustomRouteFn = Callable[[int, int, Dict[str, object]], int]


class RoutingError(RuntimeError):
    """Raised when no legal route exists."""


@dataclass
class Router:
    """Configurable routing engine for the simulator."""

    algorithm: str = "xy"
    custom_route: CustomRouteFn | None = None
    fault_tolerant: bool = True

    def path_between(self, src_tile_id: int, dst_tile_id: int, topology: Topology) -> list[int]:
        """Return a concrete path from src to dst, inclusive."""

        if src_tile_id == dst_tile_id:
            return [src_tile_id]
        if self.custom_route is not None or self.algorithm == "custom":
            return self._custom_path(src_tile_id, dst_tile_id, topology)
        path = self._bfs_path(src_tile_id, dst_tile_id, topology)
        if path is None:
            raise RoutingError(
                f"No route from tile {src_tile_id} to tile {dst_tile_id} using {self.algorithm} routing."
            )
        return path

    def _custom_path(self, src_tile_id: int, dst_tile_id: int, topology: Topology) -> list[int]:
        if self.custom_route is None:
            raise RoutingError("Custom routing requested without a custom_route callable.")
        visited = {src_tile_id}
        path = [src_tile_id]
        current = src_tile_id
        max_hops = topology.width * topology.height + 1
        while current != dst_tile_id and len(path) <= max_hops:
            next_hop = self.custom_route(
                current,
                dst_tile_id,
                {"topology": topology, "path": tuple(path)},
            )
            if next_hop in visited:
                raise RoutingError("Custom routing produced a loop.")
            if topology.get_link(current, next_hop) is None:
                raise RoutingError(
                    f"Custom routing selected illegal hop {current}->{next_hop}."
                )
            visited.add(next_hop)
            path.append(next_hop)
            current = next_hop
        if current != dst_tile_id:
            raise RoutingError("Custom routing could not reach the destination.")
        return path

    def _bfs_path(self, src_tile_id: int, dst_tile_id: int, topology: Topology) -> list[int] | None:
        queue: deque[tuple[int, list[int]]] = deque([(src_tile_id, [src_tile_id])])
        visited = {src_tile_id}
        while queue:
            node, path = queue.popleft()
            for neighbor in self._ordered_neighbors(node, dst_tile_id, topology):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                next_path = [*path, neighbor]
                if neighbor == dst_tile_id:
                    return next_path
                queue.append((neighbor, next_path))
            if not self.fault_tolerant:
                break
        return None

    def _ordered_neighbors(
        self, current_tile_id: int, dst_tile_id: int, topology: Topology
    ) -> list[int]:
        preferred_directions = self._preferred_directions(current_tile_id, dst_tile_id, topology)
        ordered: list[int] = []
        seen: set[int] = set()
        for direction in preferred_directions:
            neighbor = topology.neighbor_in_direction(current_tile_id, direction)
            if neighbor is not None and neighbor not in seen:
                ordered.append(neighbor)
                seen.add(neighbor)
        for direction in ("north", "east", "south", "west"):
            neighbor = topology.neighbor_in_direction(current_tile_id, direction)
            if neighbor is not None and neighbor not in seen:
                ordered.append(neighbor)
                seen.add(neighbor)
        return ordered

    def _preferred_directions(
        self, current_tile_id: int, dst_tile_id: int, topology: Topology
    ) -> list[str]:
        current = topology.id_to_coordinate(current_tile_id)
        dest = topology.id_to_coordinate(dst_tile_id)
        dx = self._signed_delta(current.x, dest.x, topology.width, topology.topology_type == "torus")
        dy = self._signed_delta(current.y, dest.y, topology.height, topology.topology_type == "torus")
        x_pref = "east" if dx > 0 else "west"
        y_pref = "south" if dy > 0 else "north"
        if self.algorithm == "yx":
            return self._yx_order(dx, dy, x_pref, y_pref)
        if self.algorithm == "west_first":
            return self._west_first_order(dx, dy, x_pref, y_pref)
        if self.algorithm == "north_last":
            return self._north_last_order(dx, dy, x_pref, y_pref)
        return self._xy_order(dx, dy, x_pref, y_pref)

    @staticmethod
    def _signed_delta(current: int, dest: int, size: int, wrap: bool) -> int:
        raw = dest - current
        if not wrap:
            return raw
        positive = raw % size
        negative = -((-raw) % size)
        if abs(positive) < abs(negative):
            return positive
        if abs(negative) < abs(positive):
            return negative
        return positive if positive >= 0 else negative

    @staticmethod
    def _xy_order(dx: int, dy: int, x_pref: str, y_pref: str) -> list[str]:
        if dx != 0 and dy != 0:
            return [x_pref, y_pref]
        if dx != 0:
            return [x_pref]
        if dy != 0:
            return [y_pref]
        return []

    @staticmethod
    def _yx_order(dx: int, dy: int, x_pref: str, y_pref: str) -> list[str]:
        if dx != 0 and dy != 0:
            return [y_pref, x_pref]
        if dy != 0:
            return [y_pref]
        if dx != 0:
            return [x_pref]
        return []

    @staticmethod
    def _west_first_order(dx: int, dy: int, x_pref: str, y_pref: str) -> list[str]:
        if dx < 0:
            return ["west"]
        order: list[str] = []
        if dy != 0:
            order.append(y_pref)
        if dx > 0:
            order.append("east")
        return order

    @staticmethod
    def _north_last_order(dx: int, dy: int, x_pref: str, y_pref: str) -> list[str]:
        order: list[str] = []
        if dx != 0:
            order.append(x_pref)
        if dy > 0:
            order.append("south")
        if dy < 0:
            order.append("north")
        return order
