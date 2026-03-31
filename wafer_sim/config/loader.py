"""YAML and programmatic simulator configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class TopologyConfig:
    """Topology dimensions and mode."""

    type: str = "mesh"
    width: int = 8
    height: int = 8


@dataclass
class TileConfig:
    """Tile-level resource limits."""

    injection_bandwidth: int = 32
    ejection_bandwidth: int = 32
    local_buffer_size: int = 65_536
    compute_capacity: int = 1024
    injection_queue_capacity: int = 64
    ejection_queue_capacity: int = 64


@dataclass
class LinkConfig:
    """Link-level transport properties."""

    bandwidth: int = 32
    latency: int = 1
    full_duplex: bool = True
    buffer_size: int = 1024


@dataclass
class ReticleConfig:
    """Cross-reticle link overrides."""

    enabled: bool = False
    boundary_period: int = 50
    cross_reticle_bandwidth: int | None = None
    cross_reticle_latency: int | None = None


@dataclass
class RoutingConfig:
    """Routing and deadlock-avoidance settings."""

    algorithm: str = "xy"
    virtual_channels: int = 2
    flow_control: str = "credit"
    fault_tolerant: bool = True


@dataclass
class PacketConfig:
    """Packetization settings."""

    max_packet_size: int = 256


@dataclass
class DefectsConfig:
    """Static tile and link failures."""

    dead_tiles: list[list[int]] = field(default_factory=list)
    dead_links: list[list[list[int]]] = field(default_factory=list)


@dataclass
class SimulationConfig:
    """Simulation engine options."""

    mode: str = "event_driven"
    max_cycles: int = 10_000_000
    log_level: str = "info"


@dataclass
class SimulatorConfig:
    """Top-level simulator configuration."""

    topology: TopologyConfig = field(default_factory=TopologyConfig)
    tile: TileConfig = field(default_factory=TileConfig)
    link: LinkConfig = field(default_factory=LinkConfig)
    reticle: ReticleConfig = field(default_factory=ReticleConfig)
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    packet: PacketConfig = field(default_factory=PacketConfig)
    defects: DefectsConfig = field(default_factory=DefectsConfig)
    simulation: SimulationConfig = field(default_factory=SimulationConfig)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SimulatorConfig":
        """Create a structured config from a nested dictionary."""

        return cls(
            topology=TopologyConfig(**raw.get("topology", {})),
            tile=TileConfig(**raw.get("tile", {})),
            link=LinkConfig(**raw.get("link", {})),
            reticle=ReticleConfig(**raw.get("reticle", {})),
            routing=RoutingConfig(**raw.get("routing", {})),
            packet=PacketConfig(**raw.get("packet", {})),
            defects=DefectsConfig(**raw.get("defects", {})),
            simulation=SimulationConfig(**raw.get("simulation", {})),
        )


def load_config(path: str | Path) -> SimulatorConfig:
    """Load a simulator configuration from YAML."""

    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("Simulator config YAML must contain a mapping at the root.")
    return SimulatorConfig.from_dict(raw)
