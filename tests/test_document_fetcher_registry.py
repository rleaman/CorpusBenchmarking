from __future__ import annotations

from typing import Any

import pytest

from corpus_benchmark import registry
from corpus_benchmark.builtins import register_builtins
from corpus_benchmark.metadata.document_fetcher import DocumentMetadataFetcher
from corpus_benchmark.metadata.record_store import RecordStore
from corpus_benchmark.models.config import BatteryConfig, LoaderSpec, MetricSpec, WorkspaceConfig
from corpus_benchmark.models.corpus import Document, DocumentIdentifierType
from corpus_benchmark.workspace import GlobalWorkspace


class EmptyPMIDFetcher(DocumentMetadataFetcher):
    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.PMID

    def fetch(self, identifiers: list[str]) -> list[dict[str, Any]]:
        return []


class RaisingPMIDFetcher(DocumentMetadataFetcher):
    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.PMID

    def fetch(self, identifiers: list[str]) -> list[dict[str, Any]]:
        raise RuntimeError("primary failed")


class FallbackPMIDFetcher(DocumentMetadataFetcher):
    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.PMID

    def fetch(self, identifiers: list[str]) -> list[dict[str, Any]]:
        return [
            {
                "identifiers": {DocumentIdentifierType.PMID: identifier},
                "journal": "Fallback Journal",
                "pub_year": "2025",
            }
            for identifier in identifiers
        ]


class DOIFetcher(DocumentMetadataFetcher):
    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.DOI

    def fetch(self, identifiers: list[str]) -> list[dict[str, Any]]:
        return []


def make_document_store(tmp_path):
    return RecordStore(
        tmp_path / "metadata.sqlite",
        store_name="documents",
        identifier_types={
            DocumentIdentifierType.PMID,
            DocumentIdentifierType.PMCID,
            DocumentIdentifierType.DOI,
        },
        fields={"journal", "pub_year", "journal_id", "mesh_topics"},
        field_policies={
            "journal": "strict",
            "pub_year": "strict",
            "journal_id": "strict",
            "mesh_topics": "set_union",
        },
        identifier_normalizers={
            DocumentIdentifierType.PMID: DocumentIdentifierType.PMID.normalize,
            DocumentIdentifierType.PMCID: DocumentIdentifierType.PMCID.normalize,
            DocumentIdentifierType.DOI: DocumentIdentifierType.DOI.normalize,
        },
    )


def test_register_builtins_populates_document_fetcher_registry() -> None:
    register_builtins()

    assert "pubmed_eutils" in registry.DOCUMENT_FETCHERS
    assert "pmc_eutils" in registry.DOCUMENT_FETCHERS
    assert "crossref_doi" in registry.DOCUMENT_FETCHERS


def test_workspace_config_validate_rejects_unknown_fetcher() -> None:
    config = WorkspaceConfig(
        document_fetchers={"pmid": [LoaderSpec("missing_test_fetcher")]}
    )

    with pytest.raises(ValueError, match="Unknown document fetcher"):
        config.validate()


def test_workspace_config_validate_rejects_identifier_type_mismatch(monkeypatch) -> None:
    monkeypatch.setitem(registry.DOCUMENT_FETCHERS, "test_doi_fetcher", DOIFetcher)
    config = WorkspaceConfig(
        document_fetchers={"pmid": [LoaderSpec("test_doi_fetcher")]}
    )

    with pytest.raises(ValueError, match="supports 'doi'"):
        config.validate()


def test_battery_config_validate_rejects_unknown_metric() -> None:
    config = BatteryConfig(metrics=[MetricSpec(metric_name="missing_metric")])

    with pytest.raises(ValueError, match="Unknown metric"):
        config.validate()


def test_battery_config_validate_rejects_unknown_metric_bundle() -> None:
    config = BatteryConfig(
        metrics=[
            MetricSpec(
                metric_name="document_count",
                target_bundles=["missing_bundle"],
            )
        ]
    )

    with pytest.raises(ValueError, match="unknown bundle"):
        config.validate()


def test_workspace_uses_configured_fetcher_fallbacks(monkeypatch, tmp_path) -> None:
    monkeypatch.setitem(registry.DOCUMENT_FETCHERS, "test_empty_pmid", EmptyPMIDFetcher)
    monkeypatch.setitem(registry.DOCUMENT_FETCHERS, "test_fallback_pmid", FallbackPMIDFetcher)

    config = WorkspaceConfig(
        document_store_filename=str(tmp_path / "metadata.sqlite"),
        corpora_download_dir=str(tmp_path / "corpora"),
        terminology_dir=str(tmp_path / "terminologies"),
        document_fetchers={
            "pmid": [
                LoaderSpec("test_empty_pmid"),
                LoaderSpec("test_fallback_pmid"),
            ]
        },
    )
    with make_document_store(tmp_path) as store:
        workspace = GlobalWorkspace(document_store=store, workspace_config=config)
        metadata = workspace.get_document_metadata(
            [
                Document(
                    document_id="doc-1",
                    identifiers={DocumentIdentifierType.PMID: "123"},
                )
            ]
        )

    assert metadata["doc-1"]["journal"] == "Fallback Journal"
    assert metadata["doc-1"]["pub_year"] == "2025"


def test_workspace_uses_fallback_when_primary_fetcher_raises(monkeypatch, tmp_path) -> None:
    monkeypatch.setitem(registry.DOCUMENT_FETCHERS, "test_raising_pmid", RaisingPMIDFetcher)
    monkeypatch.setitem(registry.DOCUMENT_FETCHERS, "test_fallback_pmid", FallbackPMIDFetcher)

    config = WorkspaceConfig(
        document_store_filename=str(tmp_path / "metadata.sqlite"),
        corpora_download_dir=str(tmp_path / "corpora"),
        terminology_dir=str(tmp_path / "terminologies"),
        document_fetchers={
            "pmid": [
                LoaderSpec("test_raising_pmid"),
                LoaderSpec("test_fallback_pmid"),
            ]
        },
    )
    with make_document_store(tmp_path) as store:
        workspace = GlobalWorkspace(document_store=store, workspace_config=config)
        metadata = workspace.get_document_metadata(
            [
                Document(
                    document_id="doc-1",
                    identifiers={DocumentIdentifierType.PMID: "123"},
                )
            ]
        )

    assert metadata["doc-1"]["journal"] == "Fallback Journal"
