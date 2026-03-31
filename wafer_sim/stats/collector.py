"""Statistics collection for simulator runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from math import ceil
from statistics import mean

from wafer_sim.core.packet import Packet
from wafer_sim.core.topology import Topology


def _percentile(values: list[int], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, ceil(quantile * len(ordered)) - 1))
    return float(ordered[index])


@dataclass
class PacketMetrics:
    packet_id: str
    collective_op_id: str
    group_id: str
    source_tile_id: int
    destination_tile_id: int
    size: int
    injection_time: int
    network_entry_time: int
    delivery_time: int
    latency: int
    hop_count: int
    queuing_delay: int
    path_taken: list[int]


@dataclass
class LinkMetrics:
    src_tile_id: int
    dst_tile_id: int
    utilization: float
    total_bytes_transferred: int
    busy_cycles: int
    idle_cycles: int
    peak_queue_depth: int
    bytes_by_collective: dict[str, int]


@dataclass
class TileMetrics:
    tile_id: int
    injection_stall_cycles: int
    ejection_stall_cycles: int
    idle_time: int
    active_time: int
    num_collectives: int


@dataclass
class CollectiveMetrics:
    op_id: str
    group_id: str
    op_type: str
    algorithm: str
    start_time: int
    end_time: int
    completion_time: int
    data_size: int
    group_size: int
    algorithm_bandwidth: float
    bus_bandwidth: float
    theoretical_lower_bound: int
    efficiency: float
    depends_on: list[str] = field(default_factory=list)


@dataclass
class WorkloadResults:
    workload_name: str
    topology: str
    total_completion_time: int
    op_results: list[CollectiveMetrics]
    link_metrics: list[LinkMetrics]
    tile_metrics: list[TileMetrics]
    packet_metrics: list[PacketMetrics]
    network_throughput: float
    avg_packet_latency: float
    p50_packet_latency: float
    p99_packet_latency: float
    max_packet_latency: int
    peak_link_utilization: float
    avg_link_utilization: float
    bisection_utilization: float
    critical_path_ops: list[str]
    critical_path_cycles: int

    def to_dict(self) -> dict[str, object]:
        """Convert results into JSON-serializable dictionaries."""

        return asdict(self)


class StatsCollector:
    """Aggregates runtime metrics for a workload run."""

    def __init__(self) -> None:
        self.op_records: dict[str, dict[str, object]] = {}

    def reset(self) -> None:
        """Clear accumulated state."""

        self.op_records.clear()

    def register_op(
        self,
        op_id: str,
        group_id: str,
        op_type: str,
        algorithm: str,
        data_size: int,
        group_size: int,
        depends_on: list[str],
        start_time: int,
    ) -> None:
        """Record the start of a collective op."""

        self.op_records[op_id] = {
            "op_id": op_id,
            "group_id": group_id,
            "op_type": op_type,
            "algorithm": algorithm,
            "data_size": data_size,
            "group_size": group_size,
            "depends_on": list(depends_on),
            "start_time": start_time,
            "end_time": start_time,
            "completion_time": 0,
            "algorithm_bandwidth": 0.0,
            "bus_bandwidth": 0.0,
            "theoretical_lower_bound": 0,
            "efficiency": 0.0,
        }

    def complete_op(
        self,
        op_id: str,
        end_time: int,
        bus_bandwidth: float,
        theoretical_lower_bound: int,
    ) -> None:
        """Finalize a collective op's summary metrics."""

        record = self.op_records[op_id]
        start_time = int(record["start_time"])
        completion_time = max(0, end_time - start_time)
        data_size = int(record["data_size"])
        record["end_time"] = end_time
        record["completion_time"] = completion_time
        record["algorithm_bandwidth"] = (
            float(data_size) / completion_time if completion_time else 0.0
        )
        record["bus_bandwidth"] = bus_bandwidth
        record["theoretical_lower_bound"] = theoretical_lower_bound
        record["efficiency"] = (
            float(theoretical_lower_bound) / completion_time if completion_time else 0.0
        )

    def build_results(
        self,
        workload_name: str,
        topology: Topology,
        packets: dict[str, Packet],
        total_completion_time: int,
    ) -> WorkloadResults:
        """Materialize the final results object."""

        packet_metrics = [
            PacketMetrics(
                packet_id=packet.packet_id,
                collective_op_id=packet.collective_op_id,
                group_id=packet.group_id,
                source_tile_id=packet.source_tile_id,
                destination_tile_id=packet.destination_tile_id,
                size=packet.size,
                injection_time=packet.injection_time,
                network_entry_time=packet.network_entry_time or packet.injection_time,
                delivery_time=packet.delivery_time or packet.injection_time,
                latency=(packet.delivery_time or packet.injection_time) - packet.injection_time,
                hop_count=packet.hop_count,
                queuing_delay=packet.queuing_delay,
                path_taken=list(packet.path_taken),
            )
            for packet in packets.values()
            if packet.delivery_time is not None
        ]
        packet_latencies = [metric.latency for metric in packet_metrics]
        link_metrics = []
        for link in topology.links.values():
            idle_cycles = max(0, total_completion_time - link.busy_cycles)
            utilization = (
                float(link.busy_cycles) / total_completion_time if total_completion_time else 0.0
            )
            link_metrics.append(
                LinkMetrics(
                    src_tile_id=link.src_tile_id,
                    dst_tile_id=link.dst_tile_id,
                    utilization=utilization,
                    total_bytes_transferred=link.total_bytes_transferred,
                    busy_cycles=link.busy_cycles,
                    idle_cycles=idle_cycles,
                    peak_queue_depth=link.peak_queue_depth,
                    bytes_by_collective=dict(link.bytes_by_collective),
                )
            )
        tile_metrics = [
            TileMetrics(
                tile_id=tile.tile_id,
                injection_stall_cycles=tile.injection_stall_cycles,
                ejection_stall_cycles=tile.ejection_stall_cycles,
                idle_time=max(0, total_completion_time - tile.active_cycles),
                active_time=tile.active_cycles,
                num_collectives=len(tile.collectives_participated),
            )
            for tile in topology.tiles.values()
            if not tile.defective
        ]
        op_results = [
            CollectiveMetrics(**record)
            for _, record in sorted(self.op_records.items(), key=lambda item: item[1]["start_time"])
        ]
        total_packet_bytes = sum(metric.size for metric in packet_metrics)
        network_throughput = (
            float(total_packet_bytes) / total_completion_time if total_completion_time else 0.0
        )
        link_utilizations = [metric.utilization for metric in link_metrics]
        critical_path_ops, critical_path_cycles = self._critical_path(op_results)
        return WorkloadResults(
            workload_name=workload_name,
            topology=f"{topology.width}x{topology.height} {topology.topology_type.title()}",
            total_completion_time=total_completion_time,
            op_results=op_results,
            link_metrics=link_metrics,
            tile_metrics=tile_metrics,
            packet_metrics=packet_metrics,
            network_throughput=network_throughput,
            avg_packet_latency=mean(packet_latencies) if packet_latencies else 0.0,
            p50_packet_latency=_percentile(packet_latencies, 0.50),
            p99_packet_latency=_percentile(packet_latencies, 0.99),
            max_packet_latency=max(packet_latencies, default=0),
            peak_link_utilization=max(link_utilizations, default=0.0),
            avg_link_utilization=mean(link_utilizations) if link_utilizations else 0.0,
            bisection_utilization=self._bisection_utilization(topology, total_completion_time),
            critical_path_ops=critical_path_ops,
            critical_path_cycles=critical_path_cycles,
        )

    @staticmethod
    def _critical_path(op_results: list[CollectiveMetrics]) -> tuple[list[str], int]:
        by_id = {result.op_id: result for result in op_results}
        memo: dict[str, tuple[int, list[str]]] = {}

        def visit(op_id: str) -> tuple[int, list[str]]:
            if op_id in memo:
                return memo[op_id]
            op = by_id[op_id]
            if not op.depends_on:
                memo[op_id] = (op.completion_time, [op_id])
                return memo[op_id]
            best_weight = -1
            best_path: list[str] = []
            for dependency in op.depends_on:
                dep_weight, dep_path = visit(dependency)
                if dep_weight > best_weight:
                    best_weight = dep_weight
                    best_path = dep_path
            memo[op_id] = (best_weight + op.completion_time, [*best_path, op_id])
            return memo[op_id]

        best = (0, [])
        for op in op_results:
            candidate = visit(op.op_id)
            if candidate[0] > best[0]:
                best = candidate
        return best[1], best[0]

    @staticmethod
    def _bisection_utilization(topology: Topology, total_completion_time: int) -> float:
        if total_completion_time <= 0:
            return 0.0
        crossing_links = []
        vertical_cut = topology.width // 2
        horizontal_cut = topology.height // 2
        for link in topology.links.values():
            src = topology.id_to_coordinate(link.src_tile_id)
            dst = topology.id_to_coordinate(link.dst_tile_id)
            crosses_vertical = {src.x, dst.x} == {vertical_cut - 1, vertical_cut}
            crosses_horizontal = {src.y, dst.y} == {horizontal_cut - 1, horizontal_cut}
            if crosses_vertical or crosses_horizontal:
                crossing_links.append(link)
        if not crossing_links:
            return 0.0
        total_busy = sum(link.busy_cycles for link in crossing_links)
        return float(total_busy) / (len(crossing_links) * total_completion_time)
