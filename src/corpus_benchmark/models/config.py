from __future__ import annotations

from collections import Counter
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class LoaderSpec:
    name: str
    params: dict[str, Any] = field(default_factory=dict)

@dataclass(slots=True)
class AcquisitionSpec:
    source_urls: list[str] = field(default_factory=list)
    format: str | None = None
    converter: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'AcquisitionSpec':
        urls = data.get("source_urls", [])
        # Support both 'source_url' (single) and 'source_urls' (list) for convenience
        if "source_url" in data:
            urls.append(data["source_url"])
        logger.debug("Parsed acquisition spec with %s source URLs", len(urls))
        return cls(
            source_urls=urls,
            format=data.get("format"),
            converter=data.get("converter"),
        )

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
    acquisition: AcquisitionSpec | None = None
    cache_filename: str | None = None


@dataclass(slots=True)
class WorkspaceConfig:
    """Global configuration for the benchmarking workspace and caches."""
    metadata_cache_filename: str = "data/metadata_cache.json"
    corpora_download_dir: str = "corpora/"
    terminology_dir: str = "data/terminologies/"

@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    filename: str | None = None

@dataclass(slots=True)
class BatteryConfig:
    workspace: WorkspaceConfig = field(default_factory=WorkspaceConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    corpora: dict[str, BenchmarkConfig] = field(default_factory=dict)
    terminologies: dict[str, LoaderSpec] = field(default_factory=dict)
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
        logger.debug("Validated battery config with %s metrics", len(self.metrics))
