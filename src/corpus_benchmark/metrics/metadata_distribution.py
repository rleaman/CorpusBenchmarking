from __future__ import annotations

from collections import Counter
from typing import Any

from corpus_benchmark.context import MetricTarget, get_labels, get_documents
from corpus_benchmark.registry import register_subset_metric
from corpus_benchmark.results import SubsetMetricResult

metadata_cache_filename = "cache/metadata.json"

def calculate_proportions(counts: Counter[str, int]) -> dict[str, float]:
    total = counts.total()
    return {
        label: (count / total if total else 0.0)
        for label, count in counts.items()
    }

def get_PubMed_metadata(pmids: list[str], metadata_cache_filename: str) -> dict[str: dict[str: Any]]:
    metadata = dict()
    # TODO Implement
    return metadata

def get_journals(target: MetricTarget):
    documents = get_documents(target)
    pmids = [document.document_id for document in documents]
    metadata = get_PubMed_metadata(pmids)
    journals = []
    for document in get_documents(target):
        pmid = document.document_id
    return journals

@register_subset_metric("journal_distribution")
def journal_distribution(target: MetricTarget, result_name: str) -> SubsetMetricResult:
    counts = Counter(get_journals(target))
    return SubsetMetricResult(
        result_name=result_name,
        metric_name="journal_distribution",
        value=calculate_proportions(counts),
        subset_name=target.name,
        details={
            "counts": dict(counts),
            "total": counts.total(),
        },
    )

def get_publication_years(target: MetricTarget):
    publication_years = []
    # TODO Implement
    return publication_years

@register_subset_metric("publication_year_distribution")
def publication_year_distribution(target: MetricTarget, result_name: str) -> SubsetMetricResult:
    counts = Counter(get_publication_years(target))
    return SubsetMetricResult(
        result_name=result_name,
        metric_name="publication_year_distribution",
        value=calculate_proportions(counts),
        subset_name=target.name,
        details={
            "counts": dict(counts),
            "total": counts.total(),
        },
    )
