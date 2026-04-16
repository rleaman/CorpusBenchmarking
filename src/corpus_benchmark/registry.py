from __future__ import annotations

from collections.abc import Callable
from typing import Any

LOADERS: dict[str, Callable[..., Any]] = {}
SUBSET_METRICS: dict[str, Callable[..., Any]] = {}
CROSS_METRICS: dict[str, Callable[..., Any]] = {}


def register_loader(name: str):
    """Register a corpus loader under a stable symbolic name."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in LOADERS:
            raise ValueError(f"Loader '{name}' is already registered.")
        LOADERS[name] = func
        return func

    return decorator


def register_subset_metric(name: str):
    """Register a metric under a stable symbolic name."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in SUBSET_METRICS:
            raise ValueError(f"Metric '{name}' is already registered.")
        SUBSET_METRICS[name] = func
        return func

    return decorator

def register_cross_metric(name: str):
    """Register a metric under a stable symbolic name."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in CROSS_METRICS:
            raise ValueError(f"Metric '{name}' is already registered.")
        CROSS_METRICS[name] = func
        return func

    return decorator
