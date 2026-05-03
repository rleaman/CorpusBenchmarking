from __future__ import annotations
import inspect
import logging
from typing import Any, Dict


from corpus_benchmark.builtins import register_builtins
from corpus_benchmark.metadata.document_fetcher import (
    DocumentMetadataFetcher,
)
from corpus_benchmark.metadata.eutils_client import EUtilsClient
from corpus_benchmark.metadata.record_store import RecordStore, StoredRecord
from corpus_benchmark.acquisition import AcquisitionManager
from corpus_benchmark.models.config import LoaderSpec
from corpus_benchmark.models.config import WorkspaceConfig
from corpus_benchmark.models.corpus import Document, DocumentIdentifierType
from corpus_benchmark.models.terminologies import TerminologyResource
from corpus_benchmark.registry import DOCUMENT_FETCHERS

logger = logging.getLogger(__name__)

METADATA_UPDATE_LOG_FREQUENCY = 100


class GlobalWorkspace:
    """Manages persistent, cross-run resources like caches and downloaded files."""

    document_store: RecordStore
    doc_store_preloaded: bool
    acquisition_manager: AcquisitionManager
    workspace_config: WorkspaceConfig
    terminologies: dict[str, TerminologyResource]

    def __init__(self, document_store: RecordStore, workspace_config: WorkspaceConfig):
        self.document_store = document_store
        self.doc_store_preloaded = False
        self.workspace_config = workspace_config
        self.acquisition_manager = AcquisitionManager(workspace_config)
        self.terminologies = {}
        self.fetchers = self._build_document_fetchers(workspace_config.document_fetchers)

    def preload_document_store(self):
        if self.doc_store_preloaded:
            return
        logger.debug("Preloading document store")
        self.document_store.preload()
        logger.debug("... done")

    def _build_document_fetchers(self, configured_fetchers: dict[str, list[LoaderSpec]]) -> dict[DocumentIdentifierType, list[DocumentMetadataFetcher]]:
        register_builtins()
        eutils_client = EUtilsClient()
        fetchers: dict[DocumentIdentifierType, list[DocumentMetadataFetcher]] = {}

        for raw_id_type, fetcher_specs in configured_fetchers.items():
            raw_id_type_value = getattr(raw_id_type, "value", str(raw_id_type)).lower()
            id_type = DocumentIdentifierType(raw_id_type_value)
            fetchers[id_type] = []
            for fetcher_spec in fetcher_specs:
                if fetcher_spec.name not in DOCUMENT_FETCHERS:
                    available = ", ".join(sorted(DOCUMENT_FETCHERS)) or "<none>"
                    raise ValueError(f"Unknown document fetcher '{fetcher_spec.name}' for {id_type}. " f"Available document fetchers: {available}")

                fetcher_cls = DOCUMENT_FETCHERS[fetcher_spec.name]
                params = dict(fetcher_spec.params)
                if "client" in inspect.signature(fetcher_cls).parameters and "client" not in params and not params:
                    params["client"] = eutils_client
                fetcher = fetcher_cls(**params)

                if fetcher.supported_id_type != id_type:
                    raise ValueError(f"Document fetcher '{fetcher_spec.name}' supports " f"{fetcher.supported_id_type}, but it was configured for {id_type}.")
                fetchers[id_type].append(fetcher)

        return fetchers

    @staticmethod
    def _format_stored_record(record: StoredRecord) -> Dict[str, Any]:
        metadata = dict(record.data)
        identifiers: dict[DocumentIdentifierType | str, str | list[str]] = {}
        for raw_id_type, values in record.identifiers.items():
            try:
                id_type: DocumentIdentifierType | str = DocumentIdentifierType(raw_id_type.lower())
            except ValueError:
                id_type = raw_id_type.lower()
            identifiers[id_type] = values[0] if len(values) == 1 else values
        metadata["identifiers"] = identifiers
        return metadata

    def _get_document_record(self, id_type: DocumentIdentifierType, id_val: str) -> Dict[str, Any] | None:
        record = self.document_store.get(id_type, id_val)
        if record is None:
            return None
        return self._format_stored_record(record)

    def _add_document_records(self, new_records: list[Dict[str, Any]]) -> None:
        updated = 0
        for record_index, new_record in enumerate(new_records):
            if record_index % METADATA_UPDATE_LOG_FREQUENCY == 0:
                logger.info(f"Updating metadata records: {record_index+1} of {len(new_records)}")
            identifiers = new_record.get("identifiers", {})
            data = {key: value for key, value in new_record.items() if key != "identifiers"}
            self.document_store.upsert(identifiers=identifiers, data=data)
            updated += 1
        if updated > 0:
            logger.info("Updated %s metadata records", updated)

    def _attach_known_document_identifiers(self, documents: list[Document]) -> None:
        for doc in documents:
            if not doc.identifiers:
                continue
            if any(self.document_store.get(id_type, id_val) is not None for id_type, id_val in doc.identifiers.items()):
                self.document_store.upsert(identifiers=doc.identifiers)

    def get_document_metadata(self, documents: list[Document]) -> Dict[str, Dict[str, Any]]:
        self._attach_known_document_identifiers(documents)
        missing_ids = {id_type: set() for id_type in self.fetchers.keys()}
        # 1. Check store
        for doc in documents:
            for id_type, id_val in doc.identifiers.items():
                record = self._get_document_record(id_type, id_val)
                # print(f"Metadata for {id_type}:{id_val} returned {record}")
                if not record and id_type in self.fetchers:
                    missing_ids[id_type].add(id_val)

        for id_type, missing_ids_by_type in missing_ids.items():
            logger.info("Number of %s IDs to fetch: %s", id_type, len(missing_ids_by_type))

        # 2. Fetch missing items using configured primary/fallback fetchers and add new records to the store
        for id_type, id_set in missing_ids.items():
            remaining_ids = set(id_set)
            for fetcher in self.fetchers[id_type]:
                if not remaining_ids:
                    break
                try:
                    fetched_records = fetcher.fetch(list(remaining_ids))
                except Exception as e:
                    logger.warning(
                        "Document fetcher %s failed for %s IDs of type %s: %s",
                        type(fetcher).__name__,
                        len(remaining_ids),
                        id_type,
                        e,
                    )
                    continue
                self._add_document_records(fetched_records)
                remaining_ids = {id_val for id_val in remaining_ids if self._get_document_record(id_type, id_val) is None}
            if remaining_ids:
                logger.warning(
                    "Could not fetch metadata for %s %s IDs using configured fetchers",
                    len(remaining_ids),
                    id_type,
                )

        # 3. Get metadata for realzies
        doc_metadata = {}
        for doc in documents:
            record = None
            for id_type, id_val in doc.identifiers.items():
                record = self._get_document_record(id_type, id_val)
                # print(f"Metadata for {id_type}:{id_val} returned {record}")
                if record:
                    break  # Found it!
            doc_metadata[doc.document_id] = record if record else {}

        logger.debug("Resolved metadata for %s documents", len(doc_metadata))
        return doc_metadata
