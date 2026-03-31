"""CLI entry point for wafer_sim."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from wafer_sim import Simulator
from wafer_sim.ccl import CCLAlgorithmRegistry
from wafer_sim.stats.reporter import format_summary_report
from wafer_sim.workload.collective_op import CollectiveOp
from wafer_sim.workload.comm_group import CommGroup, GroupBuilder
from wafer_sim.workload.workload import Workload


def _parse_size(value: str) -> int:
    suffixes = {"k": 1024, "m": 1024**2, "g": 1024**3}
    lowered = value.strip().lower()
    if lowered[-1] in suffixes:
        return int(float(lowered[:-1]) * suffixes[lowered[-1]])
    return int(lowered)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Wafer-scale routing simulator")
    parser.add_argument("--config", type=str, help="Hardware config YAML")
    parser.add_argument("--workload", type=str, help="Workload YAML")
    parser.add_argument("--output", type=str, help="Output directory for results")
    parser.add_argument("--list-algorithms", action="store_true", help="List available CCL algorithms")
    parser.add_argument("--topology", default="mesh", help="Topology type for quick test")
    parser.add_argument("--width", type=int, default=8, help="Topology width for quick test")
    parser.add_argument("--height", type=int, default=8, help="Topology height for quick test")
    parser.add_argument("--op", type=str, help="Quick-test collective op type")
    parser.add_argument("--group-size", type=int, help="Quick-test group size")
    parser.add_argument("--group-strategy", default="block", help="Quick-test group construction strategy")
    parser.add_argument("--algorithm", type=str, help="Quick-test algorithm name")
    parser.add_argument("--data-size", type=str, help="Quick-test bytes per tile")
    parser.add_argument("--num-chunks", type=int, default=1, help="Quick-test chunk count")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.list_algorithms:
        for name in CCLAlgorithmRegistry.list_algorithms():
            print(name)
        return 0

    if args.config:
        simulator = Simulator.from_config(args.config)
    else:
        simulator = Simulator(topology_type=args.topology, width=args.width, height=args.height)

    if args.workload:
        results = simulator.run_workload_from_yaml(args.workload)
    elif args.op and args.group_size and args.algorithm and args.data_size:
        builder = GroupBuilder()
        groups = builder.every_n_tiles(
            simulator.topology,
            n=args.group_size,
            strategy=args.group_strategy,
        )
        if not groups:
            raise ValueError("Quick test could not construct any groups.")
        workload = Workload(name="quick_test")
        group = groups[0]
        workload.add_group(group)
        workload.add_op(
            CollectiveOp(
                op_id=f"quick_{args.op}",
                op_type=args.op,
                group_id=group.group_id,
                data_size=_parse_size(args.data_size),
                algorithm=args.algorithm,
                algorithm_params={"num_chunks": args.num_chunks},
            )
        )
        results = simulator.run_workload(workload)
    else:
        parser.error("Provide either --workload or the quick-test flags.")

    print(format_summary_report(results))
    if args.output:
        simulator.export_results(results, args.output)
    return 0
