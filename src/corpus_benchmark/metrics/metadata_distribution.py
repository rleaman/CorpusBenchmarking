from __future__ import annotations

from collections import Counter
from typing import Any, Dict

from corpus_benchmark.context import MetricTarget, get_documents
from corpus_benchmark.registry import register_subset_metric
from corpus_benchmark.results import SubsetMetricResult
from src.corpus_benchmark.metadata_handler import default_metadata_cache_filename
from src.corpus_benchmark.context import get_loaded_cache

PRECISION = 8  # Number of decimal places


def get_metadata_for_target(target: MetricTarget, cache_path: str) -> Dict[str, Dict[str, Any]]:
    """Coordinates fetching by both PMID and PMCID for a given target."""
    cache = get_loaded_cache(target, cache_path)
    documents = get_documents(target)

    doc_metadata = {}
    for doc in documents:
        pmid = doc.infons.get("pmid")
        pmcid = doc.infons.get("pmcid")

        rec = None
        if pmid:
            rec = cache.get_by_pmid(pmid)
        if not rec and pmcid:
            rec = cache.get_by_pmcid(pmcid)

        doc_metadata[doc.document_id] = rec if rec else {}

    return doc_metadata


def calculate_proportions(counts: Counter[str, int]) -> Dict[str, float]:
    total = counts.total()
    return {label: (round(count / total, PRECISION) if total else 0.0) for label, count in counts.items()}


@register_subset_metric("journal_distribution")
def journal_distribution(target: MetricTarget, result_name: str, **kwargs) -> SubsetMetricResult:
    cache_path = kwargs.get("metadata_cache_filename", default_metadata_cache_filename)
    metadata = get_metadata_for_target(target, cache_path)

    journals = []
    for doc in get_documents(target):
        meta = metadata.get(doc.document_id, {})
        journals.append(meta.get("journal") or "Unknown")

    counts = Counter(journals)
    return SubsetMetricResult(
        result_name=result_name,
        metric_name="journal_distribution",
        value=calculate_proportions(counts),
        subset_name=target.name,
        details={"counts": dict(counts), "total": counts.total()},
    )


@register_subset_metric("publication_year_distribution")
def publication_year_distribution(target: MetricTarget, result_name: str, **kwargs) -> SubsetMetricResult:
    cache_path = kwargs.get("metadata_cache_filename", default_metadata_cache_filename)
    metadata = get_metadata_for_target(target, cache_path)

    years = []
    for doc in get_documents(target):
        meta = metadata.get(doc.document_id, {})
        years.append(meta.get("pub_year") or "Unknown")

    counts = Counter(years)
    return SubsetMetricResult(
        result_name=result_name,
        metric_name="publication_year_distribution",
        value=calculate_proportions(counts),
        subset_name=target.name,
        details={"counts": dict(counts), "total": counts.total()},
    )
