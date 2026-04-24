from __future__ import annotations
from typing import Any

from src.corpus_benchmark.metadata_handler import (
    MetadataCache,
    MetadataFetcher,
    PMCFetcher,
    PubMedFetcher,
    CrossrefDOIFetcher,
)
from src.corpus_benchmark.models.corpus import DocumentIdentifierType

class GlobalWorkspace:
    """Manages persistent, cross-run resources like caches and downloaded files."""

    metadata_cache: MetadataCache

    def __init__(self, metadata_cache: MetadataCache):
        self.metadata_cache = metadata_cache
        self.fetchers: dict[DocumentIdentifierType, MetadataFetcher] = {
            DocumentIdentifierType.PMID: PubMedFetcher(),
            #DocumentIdentifierType.DOI: CrossrefDOIFetcher(),
            DocumentIdentifierType.PMCID: PMCFetcher(),
        }

    def get_document_metadata(self, identifiers: dict[DocumentIdentifierType, str]) -> dict[str, Any]:
        """
        Attempts to find metadata using the provided identifiers.
        If missing, it automatically fetches, caches, and returns it.
        """
        # 1. Try cache first
        for id_type, id_val in identifiers.items():
            record = self.metadata_cache.get_metadata(id_type, id_val)
            if record:
                return record

        # 2. If not in cache, try fetching using the first supported identifier
        for id_type, id_val in identifiers.items():
            if id_type in self.fetchers:
                # Fetch returns a list of records; we only asked for one
                new_records = self.fetchers[id_type].fetch([id_val])
                if new_records:
                    self.metadata_cache.add_records(new_records)
                    return new_records[0]

        # 3. Give up
        return {}

    # Future additions:
    # acquisition_manager: AcquisitionManager
    # terminologies: dict[str, TerminologyResource]
