"""Export helpers for analysis artifacts.

This module currently focuses on machine-readable CSV export.
Heatmaps, traces, and DAG rendering can be layered on top of the same result objects later.
"""

from __future__ import annotations

import csv
from dataclasses import asdict
from pathlib import Path

from wafer_sim.stats.collector import WorkloadResults


def export_csv_tables(results: WorkloadResults, output_dir: str | Path) -> None:
    """Export key result tables as CSV."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    _write_table(output_path / "op_results.csv", results.op_results)
    _write_table(output_path / "link_metrics.csv", results.link_metrics)
    _write_table(output_path / "tile_metrics.csv", results.tile_metrics)
    _write_table(output_path / "packet_metrics.csv", results.packet_metrics)


def _write_table(path: Path, records: list[object]) -> None:
    if not records:
        path.write_text("", encoding="utf-8")
        return
    rows = [asdict(record) for record in records]
    fieldnames = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
