from __future__ import annotations

from abc import ABC, abstractmethod
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import Counter

from corpus_benchmark.models.corpus import DocumentIdentifierType

logger = logging.getLogger(__name__)

# Configuration
default_metadata_cache_filename = "data/metadata_cache.json"


class DocumentMetadataCache:
    """Handles the list-based cache storage and provides dual-indexing by PMID and PMCID."""

    def __init__(self, cache_path: str):
        self.cache_path = Path(cache_path)
        self.records: List[Dict[str, Any]] = []
        self.id_index: Dict[str, Dict[str, Any]] = {}
        self._load_cache()

    # TODO Fix cache to deduplicate on load

    def _load_cache(self):
        if self.cache_path.exists():
            try:
                logger.info(f"Loading metadata cache from {self.cache_path}")
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    self.records = json.load(f)
                    id_type_counts = Counter()
                    not_indexed_count = 0
                    for rec in self.records:
                        indexed = False
                        ids = rec["identifiers"]
                        for id_type in DocumentIdentifierType:
                            if not id_type in ids:
                                continue
                            key = self._make_key(id_type, ids[id_type])
                            self.id_index[key] = rec
                            indexed = True
                            id_type_counts[id_type] += 1
                        if not indexed:
                            not_indexed_count += 1
                logger.info(f"Loaded {len(self.id_index)} keys to {len(self.records)} records")
                for id_type, id_type_count in id_type_counts.items():
                    logger.info(f"  Loaded {id_type_count} keys of type {id_type}")
                if not_indexed_count > 0:                    
                    logger.warning(f"The number of unindexed records is {not_indexed_count}")
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Could not decode cache at {self.cache_path}. Starting fresh.")
                logger.warning(f"  Error was: {e}")
                self.records = []

    def _make_key(self, id_type: DocumentIdentifierType, id_val: str) -> str:
        id_val2 = id_type.normalize(id_val)
        return f"{id_type.value}:{id_val2}"

    def get_metadata(self, id_type: DocumentIdentifierType, id_val: str) -> Optional[Dict[str, Any]]:
        return self.id_index.get(self._make_key(id_type, id_val))

    def _add_record(self, new_record: Dict[str, Any]) -> bool:
        new_ids = new_record.get("identifiers", {})

        # Step 1: Find unique existing records (using 'not in' prevents duplicate matches)
        existing_records = []
        for id_type, id_val in new_ids.items():
            match = self.get_metadata(id_type, id_val)
            if match and match not in existing_records:
                existing_records.append(match)

        if not existing_records:
            # Truly new record
            self.records.append(new_record)
            for id_type, id_val in new_ids.items():
                self.id_index[self._make_key(id_type, id_val)] = new_record
            return True

        # Step 2: Merge identifiers and check for conflicts
        merged_identifiers = dict(new_ids)
        for existing_record in existing_records:
            for id_type, id_val in existing_record.get("identifiers", {}).items():
                if id_type not in merged_identifiers:
                    merged_identifiers[id_type] = id_val
                elif merged_identifiers[id_type] != id_val:
                    raise ValueError(
                        f"Identifier conflict between {merged_identifiers} and {existing_record['identifiers']}"
                    )

        # Step 3: Merge metadata (New record values take priority over old values)
        merged_record = {**new_record}
        for existing_record in existing_records:
            for key, val in existing_record.items():
                if key == "identifiers":
                    continue
                if key not in merged_record:
                    merged_record[key] = val

        merged_record["identifiers"] = merged_identifiers

        # Step 4: Check if anything changed compared to the original record
        # (If we matched multiple different existing records, a merge is guaranteed)
        if len(existing_records) > 1 or merged_record != existing_records[0]:

            # Clean up: Remove the old stale records from the main list
            for old_rec in existing_records:
                self.records.remove(old_rec)

            # Add the newly merged gold-standard record
            self.records.append(merged_record)

            # Point ALL associated identifiers to the new merged record
            for id_type, id_val in merged_identifiers.items():
                self.id_index[self._make_key(id_type, id_val)] = merged_record

            return True

        return False

    def add_records(self, new_records: List[Dict[str, Any]]):
        updated = 0
        for new_rec in new_records:
            if self._add_record(new_rec):
                updated += 1
        if updated > 0:
            logger.info(f"Updated {updated} metadata records")
            self._save_cache()

    def _save_cache(self):
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self.records, f, indent=2)


# Abstract Base Class for Fetchers
class DocumentMetadataFetcher(ABC):
    @property
    @abstractmethod
    def supported_id_type(self) -> DocumentIdentifierType:
        pass

    @abstractmethod
    def fetch(self, identifiers: List[str]) -> List[Dict[str, Any]]:
        """Returns standard records. Must include the 'identifiers' dict in the output."""
        pass

# TODO Refactor these classes so they represent the API & query: eUtils/efetch & eUtils/esummary
# TODO Refactor so each fetcher can support more than one ID type
# TODO Have each API grab both long & short journal names, if possible (CrossRef doesn't always have short journal name)
# TODO Add a representation and metadata for journals 
# TODO How do we make the Fetcher parameters configurable? 
# TODO How do we tell the program which Fetchers to use for which IDs?

# TODO Could we get the metadata from the article itself sometimes?
# TODO Consider adding Fetchers for more APIs, e.g., Europe PMC 
# TODO Consider adding a eUtils/esearch fetcher to support DOI lookup


