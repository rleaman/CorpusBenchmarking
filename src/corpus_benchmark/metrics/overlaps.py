from __future__ import annotations

from corpus_benchmark.context import MetricTarget, get_tokens, get_mentions, get_mention_tokens, get_identifiers
from corpus_benchmark.registry import register_cross_metric
from corpus_benchmark.results import CrossSubsetMetricResult

PRECISION = 8  # Number of decimal places


@register_cross_metric("token_overlap")
def token_overlap(target1: MetricTarget, target2: MetricTarget, result_name: str) -> CrossSubsetMetricResult:
    tokens1 = set(get_tokens(target1))
    tokens2 = set(get_tokens(target2))
    intersection = tokens1.intersection(tokens2)
    union = tokens1.union(tokens2)
    jaccard = len(intersection) / len(union) if len(union) > 0 else 0.0
    return CrossSubsetMetricResult(
        result_name=result_name,
        metric_name="token_overlap",
        value=round(jaccard, PRECISION),
        subset_name1=target1.name,
        subset_name2=target2.name,
        details={
            f"len({target1.name})": len(tokens1),
            f"len({target2.name})": len(tokens2),
            "intersection": len(intersection),
            "union": len(union),
        },
    )


@register_cross_metric("mention_overlap")
def mention_overlap(target1: MetricTarget, target2: MetricTarget, result_name: str) -> CrossSubsetMetricResult:
    mentions1 = set(get_mentions(target1))
    mentions2 = set(get_mentions(target2))
    intersection = mentions1.intersection(mentions2)
    union = mentions1.union(mentions2)
    jaccard = len(intersection) / len(union) if len(union) > 0 else 0.0
    return CrossSubsetMetricResult(
        result_name=result_name,
        metric_name="mention_overlap",
        value=round(jaccard, PRECISION),
        subset_name1=target1.name,
        subset_name2=target2.name,
        details={
            f"len({target1.name})": len(mentions1),
            f"len({target2.name})": len(mentions2),
            "intersection": len(intersection),
            "union": len(union),
        },
    )


@register_cross_metric("mention_token_overlap")
def mention_token_overlap(target1: MetricTarget, target2: MetricTarget, result_name: str) -> CrossSubsetMetricResult:
    mention_tokens1 = set(get_mention_tokens(target1))
    mention_tokens2 = set(get_mention_tokens(target2))
    intersection = mention_tokens1.intersection(mention_tokens2)
    union = mention_tokens1.union(mention_tokens2)
    jaccard = len(intersection) / len(union) if len(union) > 0 else 0.0
    return CrossSubsetMetricResult(
        result_name=result_name,
        metric_name="mention_token_overlap",
        value=round(jaccard, PRECISION),
        subset_name1=target1.name,
        subset_name2=target2.name,
        details={
            f"len({target1.name})": len(mention_tokens1),
            f"len({target2.name})": len(mention_tokens2),
            "intersection": len(intersection),
            "union": len(union),
        },
    )


@register_cross_metric("identifier_overlap")
def identifier_overlap(target1: MetricTarget, target2: MetricTarget, result_name: str) -> CrossSubsetMetricResult:
    identifiers1 = set(get_identifiers(target1))
    identifiers2 = set(get_identifiers(target2))
    intersection = identifiers1.intersection(identifiers2)
    union = identifiers1.union(identifiers2)
    jaccard = len(intersection) / len(union) if len(union) > 0 else 0.0
    return CrossSubsetMetricResult(
        result_name=result_name,
        metric_name="identifier_overlap",
        value=round(jaccard, PRECISION),
        subset_name1=target1.name,
        subset_name2=target2.name,
        details={
            f"len({target1.name})": len(identifiers1),
            f"len({target2.name})": len(identifiers2),
            "intersection": len(intersection),
            "union": len(union),
        },
    )
