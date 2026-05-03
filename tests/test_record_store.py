from __future__ import annotations

import pytest

from corpus_benchmark.metadata.record_store import (
    IdentifierConflictError,
    MetadataConflictError,
    RecordStore,
    UnknownFieldError,
)
from corpus_benchmark.models.corpus import DocumentIdentifierType


def make_document_store(tmp_path):
    return RecordStore(
        tmp_path / "records.sqlite",
        store_name="documents",
        identifier_types={
            DocumentIdentifierType.PMID,
            DocumentIdentifierType.PMCID,
            DocumentIdentifierType.DOI,
        },
        fields={"journal", "pub_year", "mesh_topics"},
        field_policies={"mesh_topics": "set_union"},
        identifier_normalizers={
            DocumentIdentifierType.PMID: DocumentIdentifierType.PMID.normalize,
            DocumentIdentifierType.PMCID: DocumentIdentifierType.PMCID.normalize,
            DocumentIdentifierType.DOI: DocumentIdentifierType.DOI.normalize,
        },
    )


def test_upsert_merges_new_identifiers_for_same_record(tmp_path) -> None:
    with make_document_store(tmp_path) as store:
        first = store.upsert(
            identifiers={
                DocumentIdentifierType.PMID: "123",
                DocumentIdentifierType.DOI: "https://doi.org/10.1000/example",
            },
            data={"journal": "Example Journal", "pub_year": "2024"},
        )

        second = store.upsert(
            identifiers={
                DocumentIdentifierType.PMID: "123",
                DocumentIdentifierType.PMCID: "456",
            },
            data={"mesh_topics": ["A", "B"]},
        )

        assert second.record_id == first.record_id
        assert store.count() == 1

        record = store.get(DocumentIdentifierType.PMCID, "PMC456")
        assert record is not None
        assert record.identifiers == {
            "DOI": ["10.1000/example"],
            "PMCID": ["PMC456"],
            "PMID": ["123"],
        }
        assert record.data == {
            "journal": "Example Journal",
            "mesh_topics": ["A", "B"],
            "pub_year": "2024",
        }


def test_upsert_rejects_identifier_bridge_between_two_records(tmp_path) -> None:
    with make_document_store(tmp_path) as store:
        store.upsert(
            identifiers={DocumentIdentifierType.PMID: "123"},
            data={"journal": "Journal A"},
        )
        store.upsert(
            identifiers={DocumentIdentifierType.DOI: "10.1000/b"},
            data={"journal": "Journal B"},
        )

        with pytest.raises(IdentifierConflictError):
            store.upsert(
                identifiers={
                    DocumentIdentifierType.PMID: "123",
                    DocumentIdentifierType.DOI: "10.1000/b",
                }
            )


def test_upsert_rejects_unknown_fields_and_strict_conflicts(tmp_path) -> None:
    with make_document_store(tmp_path) as store:
        with pytest.raises(UnknownFieldError):
            store.upsert(
                identifiers={DocumentIdentifierType.PMID: "123"},
                data={"title": "Unexpected"},
            )

        store.upsert(
            identifiers={DocumentIdentifierType.PMID: "123"},
            data={"journal": "Journal A"},
        )

        with pytest.raises(MetadataConflictError):
            store.upsert(
                identifiers={DocumentIdentifierType.PMID: "123"},
                data={"journal": "Journal B"},
            )
