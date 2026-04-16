from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# TODO rename subset -> target

@dataclass(slots=True)
class SubsetMetricResult:
    result_name: str
    metric_name: str
    subset_name: str
    value: Any
    details: dict[str, Any] = field(default_factory=dict)

@dataclass(slots=True)
class CrossSubsetMetricResult:
    result_name: str
    metric_name: str
    value: Any
    subset_name1: str
    subset_name2: str
    details: dict[str, Any] = field(default_factory=dict)
