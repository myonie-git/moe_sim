"""Registry for CCL implementations."""

from __future__ import annotations

from typing import Type


class CCLAlgorithmRegistry:
    """Runtime registry for collective algorithms."""

    _registry: dict[str, type] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator used by algorithms to register themselves."""

        def decorator(algo_cls: type) -> type:
            cls._registry[name] = algo_cls
            return algo_cls

        return decorator

    @classmethod
    def create(cls, name: str, **kwargs):
        """Instantiate a registered algorithm."""

        try:
            algorithm_class = cls._registry[name]
        except KeyError as exc:
            available = ", ".join(sorted(cls._registry))
            raise KeyError(f"Unknown algorithm '{name}'. Available: {available}") from exc
        return algorithm_class(**kwargs)

    @classmethod
    def get_algorithm_class(cls, name: str) -> Type:
        """Return the registered class for validation and introspection."""

        try:
            return cls._registry[name]
        except KeyError as exc:
            available = ", ".join(sorted(cls._registry))
            raise KeyError(f"Unknown algorithm '{name}'. Available: {available}") from exc

    @classmethod
    def list_algorithms(cls) -> list[str]:
        """List all available algorithms."""

        return sorted(cls._registry)
