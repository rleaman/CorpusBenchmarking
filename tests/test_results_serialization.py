from __future__ import annotations

from corpus_benchmark.results import CrossSubsetMetricResult, SubsetMetricResult


def test_subset_metric_result_to_dict_without_details_preserves_format() -> None:
    result = SubsetMetricResult(
        result_name="doc_count",
        metric_name="document_count",
        subset_name="BC5CDR_corpus",
        value=3,
    )

    assert result.result_key() == "BC5CDR_corpus"
    assert result.to_dict() == {
        "metric_name": "document_count",
        "value": 3,
    }


def test_subset_metric_result_to_dict_with_details_preserves_format() -> None:
    result = SubsetMetricResult(
        result_name="label_distribution",
        metric_name="label_distribution",
        subset_name="BC5CDR_corpus",
        value={"Chemical": 2},
        details={"total": 2},
    )

    assert result.to_dict() == {
        "metric_name": "label_distribution",
        "value": {"Chemical": 2},
        "details": {"total": 2},
    }


def test_cross_subset_metric_result_to_dict_preserves_format() -> None:
    result = CrossSubsetMetricResult(
        result_name="token_overlap",
        metric_name="token_overlap",
        subset_name1="train",
        subset_name2="test",
        value=0.25,
        details={"intersection": 10},
    )

    assert result.result_key() == "(train, test)"
    assert result.to_dict() == {
        "metric_name": "token_overlap",
        "value": 0.25,
        "details": {"intersection": 10},
    }
