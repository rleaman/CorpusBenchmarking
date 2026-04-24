from __future__ import annotations

from abc import ABC, abstractmethod
import datetime
import json
from pathlib import Path
import random
import re
import time
from typing import Any, Dict, List, Optional
import urllib
import xml.etree.ElementTree as ET

from src.corpus_benchmark.models.corpus import DocumentIdentifierType

# Configuration
default_metadata_cache_filename = "data/metadata_cache.json"
CHUNK_SIZE = 250
WAIT_SECONDS = 0.4
PUBMED_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id="
PMC_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pmc&id="


class MetadataCache:
    """Handles the list-based cache storage and provides dual-indexing by PMID and PMCID."""

    def __init__(self, cache_path: str):
        self.cache_path = Path(cache_path)
        self.records: List[Dict[str, Any]] = []
        self.id_index: Dict[str, Dict[str, Any]] = {}
        self._load_cache()

    def _load_cache(self):
        if self.cache_path.exists():
            try:
                print(f"Loading metadata cache from {self.cache_path}")
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    self.records = json.load(f)
                    for rec in self.records:
                        if rec.get("pmid"):
                            key = self._make_key(DocumentIdentifierType.PMID, rec["pmid"])
                            self.id_index[key] = rec
                        if rec.get("pmc"):
                            key = self._make_key(DocumentIdentifierType.PMCID, rec["pmc"])
                            self.id_index[key] = rec
                        if rec.get("doi"):
                            key = self._make_key(DocumentIdentifierType.DOI, rec["doi"])
                            self.id_index[key] = rec
                print(f"Loaded {len(self.records)} records")
            except (json.JSONDecodeError, TypeError):
                print(f"Warning: Could not decode cache at {self.cache_path}. Starting fresh.")
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
            print(f"Updated {updated} metadata records")
            self._save_cache()

    def _save_cache(self):
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self.records, f, indent=2)


# Abstract Base Class for Fetchers
class MetadataFetcher(ABC):
    @property
    @abstractmethod
    def supported_id_type(self) -> DocumentIdentifierType:
        pass

    @abstractmethod
    def fetch(self, identifiers: List[str]) -> List[Dict[str, Any]]:
        """Returns standard records. Must include the 'identifiers' dict in the output."""
        pass


class PubMedFetcher(MetadataFetcher):
    """Queries NCBI eUtils for metadata using PubMed IDs."""

    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.PMID

    def fetch(self, pmids: List[str]) -> List[Dict[str, Any]]:
        if not pmids:
            return []

        results = []
        random.shuffle(pmids)
        chunks = [pmids[i : i + CHUNK_SIZE] for i in range(0, len(pmids), CHUNK_SIZE)]
        last_request = datetime.datetime.now() - datetime.timedelta(seconds=WAIT_SECONDS)

        for i, chunk in enumerate(chunks):
            diff = (datetime.datetime.now() - last_request).total_seconds()
            if diff < WAIT_SECONDS:
                time.sleep(WAIT_SECONDS - diff)

            url = PUBMED_BASE_URL + ",".join(chunk)
            try:
                with urllib.request.urlopen(url) as response:
                    root = ET.fromstring(response.read())
                last_request = datetime.datetime.now()

                for article in root.findall("./PubmedArticle"):
                    results.append(self._parse_article(article))
                print(f"PubMed Fetcher: Processed chunk {i+1}/{len(chunks)}")
            except Exception as e:
                print(f"PubMed Fetcher Error: {e}")
        return results

    def _parse_article(self, element: ET.Element) -> Dict[str, Any]:
        pmid = element.findtext("./MedlineCitation/PMID")
        pmc = element.findtext("./PubmedData/ArticleIdList/ArticleId[@IdType='pmc']")

        journal = element.findtext("./MedlineCitation/Article/Journal/ISOAbbreviation")
        if not journal:
            journal = element.findtext("./MedlineCitation/Article/Journal/Title")

        year = element.findtext("./MedlineCitation/Article/Journal/JournalIssue/PubDate/Year")
        if not year:
            medline_date = element.findtext("./MedlineCitation/Article/Journal/JournalIssue/PubDate/MedlineDate")
            year = medline_date[:4] if medline_date else None

        pmid = DocumentIdentifierType.PMID.normalize(pmid)
        identifiers = {DocumentIdentifierType.PMID: pmid}
        if not pmc is None:
            identifiers[DocumentIdentifierType.PMCID] = DocumentIdentifierType.PMCID.normalize(pmc)
        record = {
            "identifiers": identifiers,
            "journal": journal,
            "pub_year": year,
        }
        return record


class PMCFetcher(MetadataFetcher):
    """Queries NCBI eUtils for metadata using PMC IDs."""

    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.PMCID

    def fetch(self, pmcids: List[str]) -> List[Dict[str, Any]]:
        if not pmcids:
            return []

        # Normalize: API expects numeric IDs without "PMC" prefix
        pmcids = [DocumentIdentifierType.PMCID.normalize(pmcid) for pmcid in pmcids]
        numeric_ids = [pmcid[3:]  for pmcid in pmcids]
        results = []
        chunks = [numeric_ids[i : i + CHUNK_SIZE] for i in range(0, len(numeric_ids), CHUNK_SIZE)]
        last_request = datetime.datetime.now() - datetime.timedelta(seconds=WAIT_SECONDS)

        for i, chunk in enumerate(chunks):
            diff = (datetime.datetime.now() - last_request).total_seconds()
            if diff < WAIT_SECONDS:
                time.sleep(WAIT_SECONDS - diff)

            url = PMC_BASE_URL + ",".join(chunk)
            try:
                with urllib.request.urlopen(url) as response:
                    root = ET.fromstring(response.read())
                last_request = datetime.datetime.now()

                for docsum in root.findall("./DocSum"):
                    results.append(self._parse_docsum(docsum))
                print(f"PMC Fetcher: Processed chunk {i+1}/{len(chunks)}")
            except Exception as e:
                print(f"PMC Fetcher Error: {e}")
        return results

    def _parse_docsum(self, element: ET.Element) -> Dict[str, Any]:
        # PMC esummary returns ID without prefix in <Id>
        raw_pmc = element.findtext("./Id")
        pmcid = DocumentIdentifierType.PMCID.normalize(raw_pmc)
        pmid = element.findtext("./Item[@Name='ArticleIds']/Item[@Name='pmid']")
        journal = element.findtext("./Item[@Name='Source']")

        pub_date = element.findtext("./Item[@Name='PubDate']")
        # Simple extraction of year (YYYY) from string
        year = None
        if pub_date:
            match = re.search(r"\b(19|20)\d{2}\b", pub_date)
            year = match.group(0) if match else None

        identifiers = {DocumentIdentifierType.PMCID: pmcid}
        if not pmid is None:
            identifiers[DocumentIdentifierType.PMID] = DocumentIdentifierType.PMID.normalize(pmid)
        record = {
            "identifiers": identifiers,
            "journal": journal,
            "pub_year": year,
        }
        return record


class CrossrefDOIFetcher(MetadataFetcher):
    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.DOI

    def fetch(self, dois: List[str]) -> List[Dict[str, Any]]:
        results = []
        for doi in dois:
            doi = DocumentIdentifierType.DOI.normalize(doi)
            # FIXME Finish implementation: add batching/rate limiting here
            url = f"https://api.crossref.org/works/{urllib.parse.quote(doi)}"
            try:
                with urllib.request.urlopen(url) as response:
                    data = json.loads(response.read())
                    msg = data["message"]

                    record = {
                        "identifiers": {DocumentIdentifierType.DOI: doi},
                        "journal": msg.get("container-title", [None])[0],
                        "pub_year": msg.get("issued", {}).get("date-parts", [[None]])[0][0],
                    }
                    results.append(record)
            except Exception as e:
                print(f"DOI Fetcher Error for {doi}: {e}")
        return results
