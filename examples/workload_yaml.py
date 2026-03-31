"""YAML workload example."""

from wafer_sim import Simulator


def main() -> None:
    simulator = Simulator.from_config("wafer_sim/config/default.yaml")
    results = simulator.run_workload_from_yaml(
        "examples/example_workloads/simple_allreduce.yaml"
    )
    print(results.total_completion_time)


if __name__ == "__main__":
    main()
