"""Workload execution on top of the simulator."""

from __future__ import annotations

from dataclasses import dataclass, field

from wafer_sim.ccl import CCLAlgorithmRegistry
from wafer_sim.core.event import Event, EventType
from wafer_sim.stats.collector import WorkloadResults
from wafer_sim.workload.collective_op import CollectiveOp
from wafer_sim.workload.workload import Workload


@dataclass
class StepRuntime:
    step_id: int
    pending_packets: int = 0
    started: bool = False
    completed: bool = False
    completion_scheduled: bool = False
    last_delivery_time: int = 0


@dataclass
class OpRuntime:
    op: CollectiveOp
    started: bool = False
    completed: bool = False
    start_time: int | None = None
    end_time: int | None = None
    algorithm: object | None = None
    schedule: list[object] = field(default_factory=list)
    steps: dict[int, StepRuntime] = field(default_factory=dict)


class WorkloadExecutor:
    """Executes workload ops with dependency resolution."""

    def __init__(self, simulator, workload: Workload) -> None:
        self.simulator = simulator
        self.workload = workload
        self.runtime_by_op = {op.op_id: OpRuntime(op=op) for op in workload.ops}
        self.successors: dict[str, list[str]] = {op.op_id: [] for op in workload.ops}
        for op in workload.ops:
            for dependency in op.depends_on:
                self.successors.setdefault(dependency, []).append(op.op_id)

    def execute(self) -> WorkloadResults:
        """Run the workload to completion."""

        self.workload.validate(topology=self.simulator.topology)
        self.simulator.set_external_event_handler(self.handle_event)
        for op in self.workload.ops:
            if op.depends_on:
                continue
            trigger_time = op.start_time if op.start_time is not None else 0
            self.simulator.schedule_event(
                trigger_time,
                EventType.WORKLOAD_STEP_TRIGGER,
                {"op_id": op.op_id},
            )
        self.simulator.run()
        total_completion_time = max(
            (runtime.end_time or 0 for runtime in self.runtime_by_op.values()),
            default=self.simulator.current_cycle,
        )
        return self.simulator.stats.build_results(
            workload_name=self.workload.name,
            topology=self.simulator.topology,
            packets=self.simulator._packets,
            total_completion_time=total_completion_time,
        )

    def handle_event(self, event: Event) -> None:
        """Handle workload-level events from the simulator."""

        if event.event_type == EventType.WORKLOAD_STEP_TRIGGER:
            self._start_op(str(event.payload["op_id"]), event.timestamp)
        elif event.event_type == EventType.CCL_STEP_COMPLETE:
            self._complete_step(
                op_id=str(event.payload["op_id"]),
                step_id=int(event.payload["step_id"]),
                completion_time=event.timestamp,
            )
        elif event.event_type == EventType.COLLECTIVE_OP_COMPLETE:
            self._complete_op(str(event.payload["op_id"]), event.timestamp)

    def _start_op(self, op_id: str, timestamp: int) -> None:
        runtime = self.runtime_by_op[op_id]
        if runtime.started:
            return
        if any(not self.runtime_by_op[dependency].completed for dependency in runtime.op.depends_on):
            return
        op = runtime.op
        group = self.workload.groups[op.group_id]
        ordered_tiles = group.ordered_tile_ids(self.simulator.topology)
        algorithm = CCLAlgorithmRegistry.create(
            op.algorithm,
            comm_group=group,
            data_size=op.data_size,
            num_chunks=int(op.algorithm_params.get("num_chunks", 1)),
            simulator=self.simulator,
            **{key: value for key, value in op.algorithm_params.items() if key != "num_chunks"},
        )
        if not algorithm.validate():
            raise ValueError(f"Algorithm '{op.algorithm}' failed validation for op '{op.op_id}'.")
        runtime.started = True
        runtime.start_time = timestamp
        runtime.algorithm = algorithm
        runtime.schedule = algorithm.generate_schedule()
        runtime.steps = {step.step_id: StepRuntime(step_id=step.step_id) for step in runtime.schedule}
        self.simulator.stats.register_op(
            op_id=op.op_id,
            group_id=op.group_id,
            op_type=op.op_type,
            algorithm=op.algorithm,
            data_size=op.data_size,
            group_size=len(ordered_tiles),
            depends_on=op.depends_on,
            start_time=timestamp,
        )
        for tile_id in ordered_tiles:
            self.simulator.topology.tiles[tile_id].collectives_participated.add(op.op_id)
        self._launch_ready_steps(op_id, timestamp)
        if not runtime.schedule:
            self.simulator.schedule_event(
                timestamp,
                EventType.COLLECTIVE_OP_COMPLETE,
                {"op_id": op.op_id},
            )

    def _launch_ready_steps(self, op_id: str, ready_time: int) -> None:
        runtime = self.runtime_by_op[op_id]
        ordered_tiles = runtime.algorithm.comm_group.ordered_tile_ids(self.simulator.topology)
        for step in sorted(runtime.schedule, key=lambda item: item.step_id):
            step_runtime = runtime.steps[step.step_id]
            if step_runtime.started or step_runtime.completed:
                continue
            if any(not runtime.steps[dependency].completed for dependency in step.depends_on):
                continue
            launch_time = ready_time + step.compute_before
            step_runtime.started = True
            if not step.transfers:
                step_runtime.completion_scheduled = True
                self.simulator.schedule_event(
                    launch_time,
                    EventType.CCL_STEP_COMPLETE,
                    {"op_id": op_id, "step_id": step.step_id},
                )
                continue
            for transfer in step.transfers:
                src_tile_id = ordered_tiles[transfer.src_rank]
                dst_tile_id = ordered_tiles[transfer.dst_rank]
                packet_ids = self.simulator.inject_message(
                    source_tile_id=src_tile_id,
                    destination_tile_id=dst_tile_id,
                    size=transfer.size,
                    injection_time=launch_time,
                    payload_tag=f"{op_id}:step{step.step_id}:chunk{transfer.chunk_id}",
                    collective_op_id=op_id,
                    group_id=runtime.op.group_id,
                    on_delivered=self._packet_delivery_callback(op_id, step.step_id),
                )
                step_runtime.pending_packets += len(packet_ids)
            if step_runtime.pending_packets == 0 and not step_runtime.completion_scheduled:
                step_runtime.completion_scheduled = True
                self.simulator.schedule_event(
                    launch_time,
                    EventType.CCL_STEP_COMPLETE,
                    {"op_id": op_id, "step_id": step.step_id},
                )

    def _packet_delivery_callback(self, op_id: str, step_id: int):
        def callback(packet, delivery_time: int) -> None:
            runtime = self.runtime_by_op[op_id]
            step_runtime = runtime.steps[step_id]
            step_runtime.pending_packets = max(0, step_runtime.pending_packets - 1)
            step_runtime.last_delivery_time = max(step_runtime.last_delivery_time, delivery_time)
            if step_runtime.pending_packets == 0 and not step_runtime.completion_scheduled:
                step_runtime.completion_scheduled = True
                self.simulator.schedule_event(
                    step_runtime.last_delivery_time,
                    EventType.CCL_STEP_COMPLETE,
                    {"op_id": op_id, "step_id": step_id},
                )

        return callback

    def _complete_step(self, op_id: str, step_id: int, completion_time: int) -> None:
        runtime = self.runtime_by_op[op_id]
        step_runtime = runtime.steps[step_id]
        if step_runtime.completed:
            return
        step_runtime.completed = True
        runtime.algorithm.on_step_complete(step_id)
        self._launch_ready_steps(op_id, completion_time)
        if all(step.completed for step in runtime.steps.values()):
            self.simulator.schedule_event(
                completion_time,
                EventType.COLLECTIVE_OP_COMPLETE,
                {"op_id": op_id},
            )

    def _complete_op(self, op_id: str, completion_time: int) -> None:
        runtime = self.runtime_by_op[op_id]
        if runtime.completed:
            return
        runtime.completed = True
        runtime.end_time = completion_time
        runtime.algorithm.on_complete()
        theoretical_lower_bound = runtime.algorithm.estimate_theoretical_lower_bound(runtime.schedule)
        actual_completion = max(0, completion_time - (runtime.start_time or 0))
        bus_bandwidth = runtime.algorithm.estimate_bus_bandwidth(runtime.schedule, actual_completion)
        self.simulator.stats.complete_op(
            op_id=op_id,
            end_time=completion_time,
            bus_bandwidth=bus_bandwidth,
            theoretical_lower_bound=theoretical_lower_bound,
        )
        for successor in self.successors.get(op_id, []):
            successor_runtime = self.runtime_by_op[successor]
            if successor_runtime.started:
                continue
            if any(
                not self.runtime_by_op[dependency].completed
                for dependency in successor_runtime.op.depends_on
            ):
                continue
            trigger_time = max(
                completion_time,
                successor_runtime.op.start_time if successor_runtime.op.start_time is not None else completion_time,
            )
            self.simulator.schedule_event(
                trigger_time,
                EventType.WORKLOAD_STEP_TRIGGER,
                {"op_id": successor},
            )
