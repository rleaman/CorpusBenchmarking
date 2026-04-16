from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import yaml

from corpus_benchmark.models.config import BatteryConfig, BenchmarkConfig, LoaderSpec, MetricSpec, DatasetBundle, SubsetRef, ComparisonSuite
from corpus_benchmark.models.filters import AnnotationFilter
from corpus_benchmark.runner import run_benchmark
from corpus_benchmark.results import SubsetMetricResult, CrossSubsetMetricResult

def load_benchmark_config(path: str | Path) -> BenchmarkConfig:
    with open(path, "r", encoding="utf-8") as fp:
        raw_config: dict[str, Any] = yaml.safe_load(fp)

    annotation_filters = {
        name: AnnotationFilter.from_config_dict(raw_filter)
        for name, raw_filter in raw_config.get("filters", {}).items()
    }

    return BenchmarkConfig(
        name=str(raw_config["name"]),
        loader=LoaderSpec(**raw_config["loader"]),
        annotation_filters=annotation_filters,
    )

def load_battery_config(path: str | Path) -> BatteryConfig:
    with open(path, "r", encoding="utf-8") as fp:
        raw_config: dict[str, Any] = yaml.safe_load(fp)

    # 1. Load Corpora
    corpora = {
        corpus_name: load_benchmark_config(corpus_path)
        for corpus_name, corpus_path in raw_config.get("corpora", {}).items()
    }

    # 2. Load Dataset Bundles
    bundles = {
        bundle_name: DatasetBundle(
            name=bundle_name,
            subsets=[SubsetRef(**ref_dict) for ref_dict in subset_list]
        )
        for bundle_name, subset_list in raw_config.get("bundles", {}).items()
    }

    # 3. Load Comparison Suites
    comparison_suites = {
        suite_name: ComparisonSuite(
            name=suite_name,
            # Ensure YAML lists of 2 items are safely cast to Python tuples
            bundle_pairs=[(pair[0], pair[1]) for pair in pairs_list]
        )
        for suite_name, pairs_list in raw_config.get("comparison_suites", {}).items()
    }

    # 4. Initialize the full config
    return BatteryConfig(
        corpora=corpora,
        bundles=bundles,
        comparison_suites=comparison_suites,
        metrics=[MetricSpec(**metric) for metric in raw_config.get("metrics", [])],
        output_path=raw_config.get("output_path"),
    )

def serialize_results(results) -> dict[str, list[dict[str, Any]]]:
    results_dict: dict[str, list[dict[str, Any]]] = dict()
    for result in results:
        if isinstance(result, SubsetMetricResult):
            key = result.subset_name
        elif isinstance(result, CrossSubsetMetricResult):
            key = f"({result.subset_name1}, {result.subset_name2})"
        else:
            ValueError(f"Unknown result type: result = {result}, type(result) = {type(result)}")
        if not key in results_dict:
            results_dict[key] = list()
        result_dict = {
            "metric_name": result.metric_name,
            "value": result.value,
        }
        if len(result.details) > 0:
            result_dict["details"] = result.details
        results_dict[key].append(result_dict)
    return results_dict


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python -m corpus_benchmark.cli <config.yaml>", file=sys.stderr)
        raise SystemExit(2)

    battery_config = load_battery_config(Path(sys.argv[1]))
    print(f"Loaded {len(battery_config.corpora)} corpora, {len(battery_config.metrics)} metrics")

    results = run_benchmark(battery_config)
    payload = serialize_results(results)

    if battery_config.output_path:
        output_path = Path(battery_config.output_path)
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        print(json.dumps(payload, indent=2))

    print("Done.")

if __name__ == "__main__":
    main()
