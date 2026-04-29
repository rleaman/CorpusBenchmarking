from __future__ import annotations
import logging
from typing import Any, Dict


from corpus_benchmark.metadata_handler import (
    MetadataCache,
    MetadataFetcher,
    PMCFetcher,
    PubMedFetcher,
    CrossrefDOIFetcher,
)
from corpus_benchmark.models.corpus import DocumentIdentifierType
from corpus_benchmark.acquisition import AcquisitionManager
from corpus_benchmark.models.config import WorkspaceConfig
from corpus_benchmark.models.corpus import Document


from corpus_benchmark.models.terminologies import TerminologyResource

logger = logging.getLogger(__name__)


class GlobalWorkspace:
    """Manages persistent, cross-run resources like caches and downloaded files."""

    metadata_cache: MetadataCache
    acquisition_manager: AcquisitionManager
    workspace_config: WorkspaceConfig
    terminologies: dict[str, TerminologyResource]

    def __init__(self, metadata_cache: MetadataCache, workspace_config: WorkspaceConfig):
        self.metadata_cache = metadata_cache
        self.workspace_config = workspace_config
        self.acquisition_manager = AcquisitionManager(workspace_config)
        self.terminologies = {}
        # TODO Make the fetchers configurable
        self.fetchers: dict[DocumentIdentifierType, MetadataFetcher] = {
            DocumentIdentifierType.PMID: PubMedFetcher(),
            DocumentIdentifierType.PMCID: PMCFetcher(),
            # DocumentIdentifierType.DOI: CrossrefDOIFetcher(),
        }

    def get_document_metadata(self, documents: list[Document])  -> Dict[str, Dict[str, Any]]:
        missing_ids = {id_type: set() for id_type in self.fetchers.keys()}
        # 1. Check Cache
        for doc in documents:
            for id_type, id_val in doc.identifiers.items():
                record = self.metadata_cache.get_metadata(id_type, id_val)
                #print(f"Metadata for {id_type}:{id_val} returned {record}")
                if not record and id_type in self.fetchers:
                    missing_ids[id_type].add(id_val)

        for id_type, missing_ids_by_type in missing_ids.items():
            logger.info("Number of missing %s IDs: %s", id_type, len(missing_ids_by_type))

        # 2. Fetch missing items using the appropriate fetcher and add new records to the cache
        new_records = []
        for id_type, id_set in missing_ids.items():
            if id_set:
                fetcher = self.fetchers[id_type]
                new_records.extend(fetcher.fetch(list(id_set)))
        self.metadata_cache.add_records(new_records)

        # 3. Get metadata for realzies
        doc_metadata = {}
        for doc in documents:
            record = None
            for id_type, id_val in doc.identifiers.items():
                record = self.metadata_cache.get_metadata(id_type, id_val)
                #print(f"Metadata for {id_type}:{id_val} returned {record}")
                if record:
                    break # Found it!
            doc_metadata[doc.document_id] = record if record else {}

        logger.debug("Resolved metadata for %s documents", len(doc_metadata))
        return doc_metadata

    # Future additions:
    # terminologies: dict[str, TerminologyResource]
