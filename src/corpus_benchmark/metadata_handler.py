from __future__ import annotations

import datetime
import json
import random
import re
import time
import urllib
from pathlib import Path
from typing import Any, Dict, List, Optional

import xml.etree.ElementTree as ET

# Configuration
default_metadata_cache_filename = "metadata/cache.json"
CHUNK_SIZE = 250
WAIT_SECONDS = 0.4
PUBMED_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id="
PMC_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pmc&id="


class MetadataCache:
    """Handles the list-based cache storage and provides dual-indexing by PMID and PMCID."""

    def __init__(self, cache_path: str):
        self.cache_path = Path(cache_path)
        self.records: List[Dict[str, Any]] = []
        self.pmid_index: Dict[str, Dict[str, Any]] = {}
        self.pmcid_index: Dict[str, Dict[str, Any]] = {}
        self._load_cache()

    def _load_cache(self):
        if self.cache_path.exists():
            try:
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    self.records = json.load(f)
                    for rec in self.records:
                        if rec.get("pmid"):
                            self.pmid_index[str(rec["pmid"])] = rec
                        if rec.get("pmc"):
                            # Ensure PMCID is indexed consistently (e.g., PMC123)
                            pmcid = str(rec["pmc"])
                            if not pmcid.startswith("PMC"):
                                pmcid = "PMC" + pmcid
                            self.pmcid_index[pmcid] = rec
            except (json.JSONDecodeError, TypeError):
                print(f"Warning: Could not decode cache at {self.cache_path}. Starting fresh.")
                self.records = []

    def get_by_pmid(self, pmid: str) -> Optional[Dict[str, Any]]:
        return self.pmid_index.get(str(pmid))

    def get_by_pmcid(self, pmcid: str) -> Optional[Dict[str, Any]]:
        if pmcid and not pmcid.startswith("PMC"):
            pmcid = "PMC" + pmcid
        return self.pmcid_index.get(pmcid)

    def add_records(self, new_records: List[Dict[str, Any]]):
        """Adds records to internal list and indexes, avoiding duplicates."""
        updated = False
        for rec in new_records:
            pmid = str(rec.get("pmid")) if rec.get("pmid") else None
            pmcid = str(rec.get("pmc")) if rec.get("pmc") else None
            if pmcid and not pmcid.startswith("PMC"):
                pmcid = "PMC" + pmcid

            # Check if we already have this record via either ID
            if (pmid and pmid in self.pmid_index) or (pmcid and pmcid in self.pmcid_index):
                continue

            self.records.append(rec)
            if pmid:
                self.pmid_index[pmid] = rec
            if pmcid:
                self.pmcid_index[pmcid] = rec
            updated = True

        if updated:
            self._save_cache()

    def _save_cache(self):
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self.records, f, indent=2)


class PubMedFetcher:
    """Queries NCBI eUtils for metadata using PubMed IDs."""

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

        return {
            "pmid": pmid,
            "pmc": ("PMC" + pmc) if pmc and not pmc.startswith("PMC") else pmc,
            "journal": journal,
            "pub_year": year,
        }


class PMCFetcher:
    """Queries NCBI eUtils for metadata using PMC IDs."""

    def fetch(self, pmcids: List[str]) -> List[Dict[str, Any]]:
        if not pmcids:
            return []

        # Normalize: API expects numeric IDs without "PMC" prefix
        numeric_ids = [pid[3:] if pid.upper().startswith("PMC") else pid for pid in pmcids]
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
        pmcid = ("PMC" + raw_pmc) if raw_pmc else None
        pmid = element.findtext("./Item[@Name='ArticleIds']/Item[@Name='pmid']")
        journal = element.findtext("./Item[@Name='Source']")

        pub_date = element.findtext("./Item[@Name='PubDate']")
        # Simple extraction of year (YYYY) from string
        year = None
        if pub_date:
            match = re.search(r"\b(19|20)\d{2}\b", pub_date)
            year = match.group(0) if match else None

        return {"pmid": pmid, "pmc": pmcid, "journal": journal, "pub_year": year}
