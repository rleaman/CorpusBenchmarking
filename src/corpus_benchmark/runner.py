from __future__ import annotations

from typing import Any

# Ensure built-in loaders and metrics are registered.
import corpus_benchmark.loaders  # noqa: F401
import corpus_benchmark.metrics  # noqa: F401

# NOTE: "noqa: F401" means "Ignore error 'Module imported but not used'"
from corpus_benchmark.context import BenchmarkContext, MetricTarget
from corpus_benchmark.models.config import BatteryConfig, DatasetBundle
from corpus_benchmark.models.corpus import BenchmarkCorpus
from corpus_benchmark.registry import LOADERS, SUBSET_METRICS, CROSS_METRICS
from src.corpus_benchmark.workspace import GlobalWorkspace
from src.corpus_benchmark.metadata_handler import MetadataCache, default_metadata_cache_filename


def _resolve_bundle(bundle: DatasetBundle, corpora: dict, contexts: dict) -> MetricTarget:
    """Helper to convert a DatasetBundle config into an actionable MetricTarget."""
    components = []
    for ref in bundle.subsets:
        corpus = corpora[ref.corpus_name]
        subset = corpus.subsets[ref.subset_name]
        context = contexts[ref.corpus_name]
        components.append((subset, context))
    return MetricTarget(name=bundle.name, components=components)


def run_benchmark(battery_config: BatteryConfig) -> list[Any]:
    workspace = GlobalWorkspace(metadata_cache=MetadataCache(battery_config.workspace.metadata_cache_filename))
    corpora: dict[str, BenchmarkCorpus] = dict()
    contexts: dict[str, BenchmarkContext] = dict()

    for benchmark_name, benchmark_config in battery_config.corpora.items():
        print(f"Loading corpus {benchmark_name}")
        loader_name = benchmark_config.loader.name
        if loader_name not in LOADERS:
            available = ", ".join(sorted(LOADERS)) or "<none>"
            raise ValueError(f"Unknown loader '{benchmark_config.loader.name}'. Available loaders: {available}")
        loader = LOADERS[loader_name]
        benchmark_corpus = loader(**benchmark_config.loader.params)
        document_count = sum(len(corpus_subset.documents) for corpus_subset in benchmark_corpus.subsets.values())
        print(f"Loaded {document_count} documents in {len(benchmark_corpus.subsets)} subsets")
        for filter_name, filter in benchmark_config.annotation_filters.items():
            print(f'Runner: annotation filter "{filter_name}" has definition "{filter}"')
        corpora[benchmark_name] = benchmark_corpus
        contexts[benchmark_name] = BenchmarkContext(
            workspace=workspace, annotation_filters=benchmark_config.annotation_filters
        )

    results: list[Any] = []

    print("metrics = {}".format([metric_spec.metric_name for metric_spec in battery_config.metrics]))

    for metric_spec in battery_config.metrics:
        if not metric_spec.enabled:
            continue

        print(f"Calculating metric {metric_spec.result_name}")
        if metric_spec.metric_name in SUBSET_METRICS:
            metric = SUBSET_METRICS[metric_spec.metric_name]
            for bundle_name in metric_spec.target_bundles:
                bundle = battery_config.bundles[bundle_name]
                target = _resolve_bundle(bundle, corpora, contexts)

                # Execute metric
                result = metric(target, metric_spec.result_name, **getattr(metric_spec, "params", {}))
                results.append(result)
        elif metric_spec.metric_name in CROSS_METRICS:
            metric = CROSS_METRICS[metric_spec.metric_name]
            suite = battery_config.comparison_suites[metric_spec.comparison_suite]
            for bundle1_name, bundle2_name in suite.bundle_pairs:
                bundle1 = battery_config.bundles[bundle1_name]
                bundle2 = battery_config.bundles[bundle2_name]

                target1 = _resolve_bundle(bundle1, corpora, contexts)
                target2 = _resolve_bundle(bundle2, corpora, contexts)

                # Execute cross-metric (requires metrics designed for two targets)
                result = metric(target1, target2, metric_spec.result_name, **getattr(metric_spec, "params", {}))
                results.append(result)
        else:
            available_metrics = []
            available_metrics.extend(SUBSET_METRICS)
            available_metrics.extend(CROSS_METRICS)
            available = ", ".join(sorted(available_metrics)) or "<none>"
            raise ValueError(f"Unknown metric '{metric_spec.metric_name}'. Available metrics: {available}")

    # Display context usage
    print("Context usage:")
    for benchmark_name, benchmark_context in contexts.items():
        print(f"{benchmark_name}:")
        for context_key, usage_count in benchmark_context.usage_counts.items():
            print(f"\t{context_key}: {usage_count}")

    return results
