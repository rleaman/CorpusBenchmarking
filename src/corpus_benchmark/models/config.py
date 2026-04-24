from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class LoaderSpec:
    name: str
    params: dict[str, Any] = field(default_factory=dict)

@dataclass(slots=True)
class SubsetRef:
    """Explicit pointer to a specific subset within a specific corpus."""
    corpus_name: str
    subset_name: str

@dataclass(slots=True)
class DatasetBundle:
    """A logical grouping of subsets to be treated as a single unit by a metric."""
    name: str
    subsets: list[SubsetRef] = field(default_factory=list)

@dataclass(slots=True)
class ComparisonSuite:
    """A named collection defining pairs of DatasetBundles to compare."""
    name: str
    bundle_pairs: list[tuple[str, str]] = field(default_factory=list)

@dataclass(slots=True)
class MetricSpec:
    metric_name: str
    result_name: str | None = None
    
    # Replaced 'subsets' with explicit bundle or comparison suite references
    target_bundles: list[str] | None = None
    comparison_suite: str | None = None
    
    enabled: bool = True
    params: dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.result_name is None:
            self.result_name = self.metric_name

@dataclass(slots=True)
class BenchmarkConfig:
    name: str
    loader: LoaderSpec
    annotation_filters: dict[str, dict[str, Any]] = field(default_factory=dict)

@dataclass(slots=True)
class WorkspaceConfig:
    """Global configuration for the benchmarking workspace and caches."""
    metadata_cache_filename: str = "data/metadata_cache.json"
    
    # TODO This is also exactly where you'll put download/terminology settings later:
    # corpora_download_dir: str = "corpora/"
    # terminology_dir: str = "data/terminologies/"

@dataclass(slots=True)
class BatteryConfig:
    workspace: WorkspaceConfig = field(default_factory=WorkspaceConfig)
    corpora: dict[str, BenchmarkConfig] = field(default_factory=dict)
    bundles: dict[str, DatasetBundle] = field(default_factory=dict)
    comparison_suites: dict[str, ComparisonSuite] = field(default_factory=dict)    
    metrics: list[MetricSpec] = field(default_factory=list)
    output_path: str | None = None

    def __post_init__(self):
        # Ensure all metric result_names are unique
        metric_result_name_counts = Counter(metric.result_name for metric in self.metrics)
        non_unique = {name for name, count in metric_result_name_counts.items() if count > 1}
        if len(non_unique) > 0:
            raise ValueError(f"Metric result names must be unique: {non_unique}")

