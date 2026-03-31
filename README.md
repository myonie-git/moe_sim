# wafer_sim

Event-driven wafer-scale routing simulator for 2D mesh/torus chips, aimed at collective communication experiments.

## Implemented

- 2D mesh and torus topology construction with defective tile/link injection
- Directional links with bandwidth/latency and optional reticle-crossing overrides
- Deterministic XY/YX plus west-first and custom routing hooks
- Event-driven packet simulation with per-packet path, hop, and latency tracking
- Communication groups and group builders
- Built-in `ring_allreduce`, `ring_allgather`, `ring_reduce_scatter`, and `naive_all_to_all`
- Workload DAG execution with YAML glob expansion
- Summary reporting plus JSON/CSV export
- CLI quick test mode and YAML workload mode

## Quick Start

List algorithms:

```bash
python3 -m wafer_sim --list-algorithms
```

Run a quick ring AllReduce:

```bash
python3 -m wafer_sim \
  --topology mesh --width 4 --height 4 \
  --op allreduce --group-size 4 --group-strategy block \
  --algorithm ring_allreduce --data-size 1M --num-chunks 4
```

Run from YAML:

```bash
python3 -m wafer_sim \
  --config wafer_sim/config/default.yaml \
  --workload examples/example_workloads/simple_allreduce.yaml \
  --output results/simple_allreduce
```

## Notes

- This environment defaults `python` to Python 2.7. Use `python3`.
- The current implementation focuses on the Phases 1-3 core path plus basic metrics/export. Credit-based flow control, advanced deadlock handling, and visualization are left as follow-on work.
