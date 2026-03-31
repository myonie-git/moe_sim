"""Human-readable summary reporting."""

from __future__ import annotations

from wafer_sim.stats.collector import WorkloadResults


def format_summary_report(results: WorkloadResults) -> str:
    """Render a compact stdout-friendly workload summary."""

    lines = [
        f"=== Workload Summary: {results.workload_name} ===",
        f"Topology: {results.topology}",
        f"Total completion time: {results.total_completion_time} cycles",
        "",
        "--- Per-Operation Results ---",
        "  Op                      | Group        | Algorithm        | Time (cyc) | AlgBW (B/c) | Efficiency",
    ]
    for op in results.op_results:
        lines.append(
            "  "
            f"{op.op_id[:22]:<22} | "
            f"{op.group_id[:12]:<12} | "
            f"{op.algorithm[:16]:<16} | "
            f"{op.completion_time:>10} | "
            f"{op.algorithm_bandwidth:>11.2f} | "
            f"{op.efficiency * 100:>9.2f}%"
        )
    lines.extend(
        [
            "",
            "--- Critical Path ---",
            "  " + " -> ".join(results.critical_path_ops) if results.critical_path_ops else "  <none>",
            f"  Total critical path: {results.critical_path_cycles} cycles",
            "",
            "--- Network Stats ---",
            f"  Peak link utilization: {results.peak_link_utilization * 100:.2f}%",
            f"  Avg link utilization: {results.avg_link_utilization * 100:.2f}%",
            f"  Max packet latency: {results.max_packet_latency} cycles",
            f"  P99 packet latency: {results.p99_packet_latency:.0f} cycles",
            f"  Bisection utilization: {results.bisection_utilization * 100:.2f}%",
        ]
    )
    return "\n".join(lines)
