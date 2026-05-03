from __future__ import annotations

import logging
import collections
from typing import Dict, Iterable

from corpus_benchmark.context import MetricTarget, get_identifiers
from corpus_benchmark.models.terminologies import TerminologyResource
from corpus_benchmark.registry import register_terminology_metric
from corpus_benchmark.results import SubsetMetricResult

logger = logging.getLogger(__name__)

PRECISION = 8

# TODO make these metrics resource-aware: only do ID lookups in the associated terminology
# TODO report IDs not found as <resource>:<accession>


def _tree_depth(tree_number: str) -> int:
    return len(tree_number.split("."))


def _count_by_branch(terminology: TerminologyResource, ids: Iterable[str]) -> Dict[str, float]:
    counts = collections.defaultdict(float)
    for ui in ids:
        concepts = terminology.resolve_to_tree_concepts(ui)
        if not concepts:
            continue
        keys = []
        for concept in concepts:
            for tree in concept.tree_numbers:
                keys.append(tree.split(".")[0])
        if not keys:
            continue
        weight = 1.0 / len(keys)
        for key in keys:
            counts[key] += weight
    return dict(sorted(counts.items()))


def _count_by_depth(terminology: TerminologyResource, ids: Iterable[str]) -> Dict[int, float]:
    counts = collections.defaultdict(float)
    for ui in ids:
        concepts = terminology.resolve_to_tree_concepts(ui)
        if not concepts:
            continue
        depths = [_tree_depth(tree) for concept in concepts for tree in concept.tree_numbers]
        if not depths:
            continue
        weight = 1.0 / len(depths)
        for depth in depths:
            counts[depth] += weight
    return dict(sorted(counts.items()))


def _get_global_counts_by_branch(terminology: TerminologyResource) -> Dict[str, float]:
    # Only count concepts with tree numbers to avoid double counting mapped SCRs
    target_ids = [c.ui for c in terminology.concepts.values() if c.tree_numbers]
    return _count_by_branch(terminology, target_ids)


def _get_global_counts_by_depth(terminology: TerminologyResource) -> Dict[int, float]:
    target_ids = [c.ui for c in terminology.concepts.values() if c.tree_numbers]
    return _count_by_depth(terminology, target_ids)


@register_terminology_metric("high_level_concept_counts")
def high_level_concept_counts(target: MetricTarget, result_name: str, terminology: TerminologyResource, annotation_filter_name: str | None = None, **params) -> SubsetMetricResult:
    ids = [ui for ui in get_identifiers(target, annotation_filter_name) if ui is not None]
    missing_ids = [ui for ui in ids if terminology.get_concept(ui) is None]
    missing_ids = sorted(list(set(missing_ids)))

    corpus_counts = _count_by_branch(terminology, ids)
    global_counts = _get_global_counts_by_branch(terminology)

    all_branches = sorted(set(corpus_counts.keys()) | set(global_counts.keys()))
    rows = []
    for branch_code in all_branches:
        # Find a concept that has this tree number to get its name
        label = None
        for concept in terminology.concepts.values():
            if branch_code in concept.tree_numbers:
                label = concept.name
                break

        count = corpus_counts.get(branch_code, 0.0)
        mesh_total = global_counts.get(branch_code, 0.0)
        proportion = count / mesh_total if mesh_total > 0 else 0.0

        rows.append(
            {
                "branch_code": branch_code,
                "label": label,
                "treetop": branch_code[0],
                "treetop_name": terminology.treetop_names.get(branch_code[0]),
                "count": round(count, PRECISION),
                "mesh_total_count": round(mesh_total, PRECISION),
                "proportion": round(proportion, PRECISION),
            }
        )

    return SubsetMetricResult(
        result_name=result_name,
        metric_name="high_level_concept_counts",
        subset_name=target.name,
        value=rows,
        details={
            "n_input_ids": len(ids),
            "n_missing_ids": len(missing_ids),
            "missing_ids": missing_ids,
        },
    )


@register_terminology_metric("concept_depth_counts")
def concept_depth_counts(target: MetricTarget, result_name: str, terminology: TerminologyResource, annotation_filter_name: str | None = None, **params) -> SubsetMetricResult:
    ids = [ui for ui in get_identifiers(target, annotation_filter_name) if ui is not None]

    corpus_counts = _count_by_depth(terminology, ids)
    global_counts = _get_global_counts_by_depth(terminology)

    all_depths = sorted(set(corpus_counts.keys()) | set(global_counts.keys()))
    rows = []
    for d in all_depths:
        c_count = corpus_counts.get(d, 0.0)
        m_count = global_counts.get(d, 0.0)
        rows.append(
            {
                "depth": d,
                "count": round(c_count, PRECISION),
                "mesh_total_count": round(m_count, PRECISION),
                "proportion": round(c_count / m_count, PRECISION) if m_count > 0 else 0.0,
            }
        )

    return SubsetMetricResult(result_name=result_name, metric_name="concept_depth_counts", subset_name=target.name, value=rows)
