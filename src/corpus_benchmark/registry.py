from __future__ import annotations

from collections.abc import Callable
from typing import Any

LOADERS: dict[str, Callable[..., Any]] = {}
TERMINOLOGY_LOADERS: dict[str, Callable[..., Any]] = {}
CONVERTERS: dict[str, Callable[..., Any]] = {}
SUBSET_METRICS: dict[str, Callable[..., Any]] = {}
CROSS_METRICS: dict[str, Callable[..., Any]] = {}
TERMINOLOGY_METRICS: dict[str, Callable[..., Any]] = {}


def register_loader(name: str):
    """Register a corpus loader under a stable symbolic name."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in LOADERS:
            raise ValueError(f"Loader '{name}' is already registered.")
        LOADERS[name] = func
        return func

    return decorator


def register_terminology_loader(name: str):
    """Register a terminology loader under a stable symbolic name."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in TERMINOLOGY_LOADERS:
            raise ValueError(f"Terminology loader '{name}' is already registered.")
        TERMINOLOGY_LOADERS[name] = func
        return func

    return decorator


def register_converter(name: str):
    """Register a custom acquisition converter/mapper."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in CONVERTERS:
            raise ValueError(f"Converter '{name}' is already registered.")
        CONVERTERS[name] = func
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


def register_terminology_metric(name: str):
    """Register a terminology metric under a stable symbolic name."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in TERMINOLOGY_METRICS:
            raise ValueError(f"Terminology metric '{name}' is already registered.")
        TERMINOLOGY_METRICS[name] = func
        return func

    return decorator
