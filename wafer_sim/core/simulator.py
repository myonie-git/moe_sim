"""Event-driven simulator core."""

from __future__ import annotations

import heapq
import json
import math
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from wafer_sim.config.loader import SimulatorConfig, load_config
from wafer_sim.core.event import Event, EventType
from wafer_sim.core.packet import Packet
from wafer_sim.core.router import CustomRouteFn, Router
from wafer_sim.core.topology import Topology
from wafer_sim.stats.collector import StatsCollector, WorkloadResults
from wafer_sim.stats.reporter import format_summary_report
from wafer_sim.stats.visualizer import export_csv_tables

if TYPE_CHECKING:
    from wafer_sim.workload.comm_group import CommGroup
    from wafer_sim.workload.workload import Workload


PacketDeliveredCallback = Callable[[Packet, int], None]


class Simulator:
    """Wafer-scale routing simulator with an event queue."""

    def __init__(
        self,
        topology_type: str = "mesh",
        width: int = 8,
        height: int = 8,
        *,
        tile: dict[str, object] | None = None,
        link: dict[str, object] | None = None,
        reticle: dict[str, object] | None = None,
        routing: dict[str, object] | None = None,
        packet: dict[str, object] | None = None,
        defects: dict[str, object] | None = None,
        simulation: dict[str, object] | None = None,
        config: SimulatorConfig | None = None,
        custom_route: CustomRouteFn | None = None,
    ) -> None:
        self.config = config or SimulatorConfig.from_dict(
            {
                "topology": {"type": topology_type, "width": width, "height": height},
                "tile": tile or {},
                "link": link or {},
                "reticle": reticle or {},
                "routing": routing or {},
                "packet": packet or {},
                "defects": defects or {},
                "simulation": simulation or {},
            }
        )
        self.topology = Topology(
            topology_type=self.config.topology.type,
            width=self.config.topology.width,
            height=self.config.topology.height,
            tile_config=self.config.tile,
            link_config=self.config.link,
            reticle_config=self.config.reticle,
            defects_config=self.config.defects,
        )
        self.router = Router(
            algorithm=self.config.routing.algorithm,
            custom_route=custom_route,
            fault_tolerant=self.config.routing.fault_tolerant,
        )
        self.stats = StatsCollector()
        self.current_cycle = 0
        self._sequence = 0
        self._packet_counter = 0
        self._events: list[Event] = []
        self._packets: dict[str, Packet] = {}
        self._packet_callbacks: dict[str, PacketDeliveredCallback] = {}
        self._external_event_handler: Callable[[Event], None] | None = None

    @classmethod
    def from_config(cls, path: str | Path) -> "Simulator":
        """Instantiate a simulator from YAML."""

        return cls(config=load_config(path))

    def reset_runtime(self) -> None:
        """Reset simulator state before a new run."""

        self.topology.reset_runtime()
        self.stats.reset()
        self.current_cycle = 0
        self._sequence = 0
        self._packet_counter = 0
        self._events.clear()
        self._packets.clear()
        self._packet_callbacks.clear()
        self._external_event_handler = None

    def schedule_event(
        self, timestamp: int, event_type: EventType, payload: dict[str, object] | None = None
    ) -> None:
        """Push a new event into the priority queue."""

        self._sequence += 1
        heapq.heappush(
            self._events,
            Event(timestamp=timestamp, sequence=self._sequence, event_type=event_type, payload=payload or {}),
        )

    def tick(self) -> int:
        """Advance the simulation by one cycle in cycle-accurate mode."""

        target = self.current_cycle + 1
        self.run_until(target)
        return self.current_cycle

    def run_until(self, cycle: int) -> int:
        """Run until the given cycle or until the event queue is drained."""

        max_cycles = min(cycle, self.config.simulation.max_cycles)
        if self.config.simulation.mode == "cycle_accurate":
            while self.current_cycle <= max_cycles:
                while self._events and self._events[0].timestamp <= self.current_cycle:
                    self._dispatch(heapq.heappop(self._events))
                if self.current_cycle == max_cycles:
                    break
                self.current_cycle += 1
            return self.current_cycle
        while self._events and self._events[0].timestamp <= max_cycles:
            self._dispatch(heapq.heappop(self._events))
        self.current_cycle = max(self.current_cycle, max_cycles)
        return self.current_cycle

    def run(self) -> int:
        """Drain the event queue."""

        limit = self.config.simulation.max_cycles
        if self.config.simulation.mode == "cycle_accurate":
            return self.run_until(limit)
        while self._events:
            event = heapq.heappop(self._events)
            if event.timestamp > limit:
                raise RuntimeError(
                    f"Simulation exceeded max_cycles={self.config.simulation.max_cycles}."
                )
            self._dispatch(event)
        return self.current_cycle

    def inject_message(
        self,
        source_tile_id: int,
        destination_tile_id: int,
        size: int,
        injection_time: int,
        payload_tag: str,
        collective_op_id: str,
        group_id: str,
        on_delivered: PacketDeliveredCallback | None = None,
    ) -> list[str]:
        """Split a message into packets and enqueue injection events."""

        if size <= 0:
            return []
        packet_ids: list[str] = []
        remaining = size
        while remaining > 0:
            packet_size = min(remaining, self.config.packet.max_packet_size)
            packet_id = f"pkt_{self._packet_counter}"
            self._packet_counter += 1
            packet = Packet(
                packet_id=packet_id,
                source_tile_id=source_tile_id,
                destination_tile_id=destination_tile_id,
                size=packet_size,
                injection_time=injection_time,
                payload_tag=payload_tag,
                collective_op_id=collective_op_id,
                group_id=group_id,
                path_taken=[source_tile_id],
                planned_path=self.router.path_between(source_tile_id, destination_tile_id, self.topology),
            )
            self._packets[packet_id] = packet
            if on_delivered is not None:
                self._packet_callbacks[packet_id] = on_delivered
            self.schedule_event(
                injection_time,
                EventType.PACKET_INJECT,
                {"packet_id": packet_id},
            )
            packet_ids.append(packet_id)
            remaining -= packet_size
        return packet_ids

    def set_external_event_handler(self, handler: Callable[[Event], None] | None) -> None:
        """Register a callback for workload-level event types."""

        self._external_event_handler = handler

    def run_single_collective(
        self,
        group: "CommGroup",
        op_type: str,
        data_size: int,
        algorithm: str,
        algorithm_params: dict[str, object] | None = None,
    ):
        """Run a one-op workload and return its result record."""

        from wafer_sim.workload.collective_op import CollectiveOp
        from wafer_sim.workload.workload import Workload

        group.validate(self.topology)
        workload = Workload(name="single_collective")
        workload.add_group(group)
        workload.add_op(
            CollectiveOp(
                op_id="single_op",
                op_type=op_type,
                group_id=group.group_id,
                data_size=data_size,
                algorithm=algorithm,
                algorithm_params=algorithm_params or {},
            )
        )
        results = self.run_workload(workload)
        return results.op_results[0]

    def run_workload(self, workload: "Workload") -> WorkloadResults:
        """Execute a workload and return aggregated results."""

        from wafer_sim.workload.executor import WorkloadExecutor

        self.reset_runtime()
        executor = WorkloadExecutor(simulator=self, workload=workload)
        return executor.execute()

    def run_workload_from_yaml(self, path: str | Path) -> WorkloadResults:
        """Load and execute a workload YAML file."""

        from wafer_sim.workload.yaml_parser import load_workload_from_yaml

        workload = load_workload_from_yaml(path, self.topology)
        return self.run_workload(workload)

    def export_results(self, results: WorkloadResults, output_dir: str | Path) -> None:
        """Write summary artifacts to disk."""

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        with (output_path / "results.json").open("w", encoding="utf-8") as handle:
            json.dump(results.to_dict(), handle, indent=2, ensure_ascii=False)
        with (output_path / "summary.txt").open("w", encoding="utf-8") as handle:
            handle.write(format_summary_report(results))
        export_csv_tables(results, output_path)

    def _dispatch(self, event: Event) -> None:
        self.current_cycle = max(self.current_cycle, event.timestamp)
        if event.event_type == EventType.PACKET_INJECT:
            self._handle_packet_inject(event)
            return
        if event.event_type == EventType.PACKET_ARRIVE_AT_ROUTER:
            self._handle_packet_arrival(event)
            return
        if event.event_type == EventType.PACKET_DELIVER:
            self._handle_packet_delivery(event)
            return
        if event.event_type == EventType.PACKET_TRAVERSE_LINK:
            return
        if self._external_event_handler is not None:
            self._external_event_handler(event)

    def _handle_packet_inject(self, event: Event) -> None:
        packet = self._packets[str(event.payload["packet_id"])]
        tile = self.topology.tiles[packet.source_tile_id]
        injection_cycles = math.ceil(packet.size / tile.injection_bandwidth)
        start = max(event.timestamp, tile.injection_available_at)
        tile.injection_stall_cycles += max(0, start - event.timestamp)
        tile.injection_available_at = start + injection_cycles
        tile.active_cycles += injection_cycles
        tile.collectives_participated.add(packet.collective_op_id)
        packet.network_entry_time = start
        self.schedule_event(
            start,
            EventType.PACKET_ARRIVE_AT_ROUTER,
            {"packet_id": packet.packet_id, "tile_id": packet.source_tile_id, "path_index": 0},
        )

    def _handle_packet_arrival(self, event: Event) -> None:
        packet = self._packets[str(event.payload["packet_id"])]
        tile_id = int(event.payload["tile_id"])
        path_index = int(event.payload["path_index"])
        if tile_id == packet.destination_tile_id:
            self.schedule_event(
                event.timestamp,
                EventType.PACKET_DELIVER,
                {"packet_id": packet.packet_id, "tile_id": tile_id},
            )
            return
        next_tile_id = packet.planned_path[path_index + 1]
        link = self.topology.get_link(tile_id, next_tile_id)
        if link is None:
            raise RuntimeError(f"Missing link from {tile_id} to {next_tile_id}.")
        start = max(event.timestamp, link.available_at)
        serialization_cycles = math.ceil(packet.size / link.bandwidth)
        if start > event.timestamp:
            packet.queuing_delay += start - event.timestamp
        if start > event.timestamp:
            link.peak_queue_depth = max(link.peak_queue_depth, 1)
        link.available_at = start + serialization_cycles
        link.busy_cycles += serialization_cycles
        link.total_bytes_transferred += packet.size
        link.bytes_by_collective[packet.collective_op_id] = (
            link.bytes_by_collective.get(packet.collective_op_id, 0) + packet.size
        )
        packet.hop_count += 1
        packet.path_taken.append(next_tile_id)
        self.schedule_event(
            start,
            EventType.PACKET_TRAVERSE_LINK,
            {
                "packet_id": packet.packet_id,
                "src_tile_id": tile_id,
                "dst_tile_id": next_tile_id,
            },
        )
        self.schedule_event(
            start + serialization_cycles + link.latency,
            EventType.PACKET_ARRIVE_AT_ROUTER,
            {
                "packet_id": packet.packet_id,
                "tile_id": next_tile_id,
                "path_index": path_index + 1,
            },
        )

    def _handle_packet_delivery(self, event: Event) -> None:
        packet = self._packets[str(event.payload["packet_id"])]
        tile = self.topology.tiles[packet.destination_tile_id]
        ejection_cycles = math.ceil(packet.size / tile.ejection_bandwidth)
        start = max(event.timestamp, tile.ejection_available_at)
        tile.ejection_stall_cycles += max(0, start - event.timestamp)
        tile.ejection_available_at = start + ejection_cycles
        tile.active_cycles += ejection_cycles
        tile.collectives_participated.add(packet.collective_op_id)
        packet.delivery_time = start + ejection_cycles
        callback = self._packet_callbacks.get(packet.packet_id)
        if callback is not None:
            callback(packet, packet.delivery_time)
