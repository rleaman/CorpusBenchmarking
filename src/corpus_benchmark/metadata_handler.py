from __future__ import annotations

from abc import ABC, abstractmethod
import datetime
import json
import logging
from pathlib import Path
import random
import re
import time
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter

from corpus_benchmark.models.corpus import DocumentIdentifierType

logger = logging.getLogger(__name__)

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
                        added = self._add_record(rec)
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
class MetadataFetcher(ABC):
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


class PubMedFetcher(MetadataFetcher):
    """Queries NCBI eUtils for metadata using PubMed IDs."""

    last_request: datetime.datetime = datetime.datetime.now() - datetime.timedelta(seconds=WAIT_SECONDS)

    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.PMID

    def fetch(self, pmids: List[str]) -> List[Dict[str, Any]]:
        if not pmids:
            return []

        results = []
        random.shuffle(pmids)
        chunks = [pmids[i : i + CHUNK_SIZE] for i in range(0, len(pmids), CHUNK_SIZE)]

        for i, chunk in enumerate(chunks):
            diff = (datetime.datetime.now() - self.last_request).total_seconds()
            if diff < WAIT_SECONDS:
                time.sleep(WAIT_SECONDS - diff)

            url = PUBMED_BASE_URL + ",".join(chunk)
            try:
                with urllib.request.urlopen(url) as response:
                    root = ET.fromstring(response.read())
                self.last_request = datetime.datetime.now()

                for article in root.findall("./PubmedArticle"):
                    results.append(self._parse_article(article))
                logger.info("PubMed Fetcher: processed chunk %s/%s", i + 1, len(chunks))
            except Exception as e:
                logger.error("PubMed Fetcher error: %s", e)
        return results

    def _parse_article(self, element: ET.Element) -> Dict[str, Any]:
        pmid = element.findtext("./MedlineCitation/PMID")
        pmc = element.findtext("./PubmedData/ArticleIdList/ArticleId[@IdType='pmc']")
        doi = element.findtext("./PubmedData/ArticleIdList/ArticleId[@IdType='doi']")

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
        if not doi is None:
            identifiers[DocumentIdentifierType.DOI] = DocumentIdentifierType.DOI.normalize(doi)
        record = {
            "identifiers": identifiers,
            "journal": journal,
            "pub_year": year,
        }
        #print(f"PubMedFetcher._parse_article() returning: {record}")
        return record


class PMCFetcher(MetadataFetcher):
    """Queries NCBI eUtils for metadata using PMC IDs."""

    last_request: datetime.datetime = datetime.datetime.now() - datetime.timedelta(seconds=WAIT_SECONDS)

    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.PMCID

    def fetch(self, pmcids: List[str]) -> List[Dict[str, Any]]:
        if not pmcids:
            return []

        # Normalize: API expects numeric IDs without "PMC" prefix
        pmcids = [DocumentIdentifierType.PMCID.normalize(pmcid) for pmcid in pmcids]
        numeric_ids = [pmcid[3:] for pmcid in pmcids]
        results = []
        chunks = [numeric_ids[i : i + CHUNK_SIZE] for i in range(0, len(numeric_ids), CHUNK_SIZE)]

        for i, chunk in enumerate(chunks):
            diff = (datetime.datetime.now() - self.last_request).total_seconds()
            if diff < WAIT_SECONDS:
                time.sleep(WAIT_SECONDS - diff)

            url = PMC_BASE_URL + ",".join(chunk)
            try:
                with urllib.request.urlopen(url) as response:
                    root = ET.fromstring(response.read())
                self.last_request = datetime.datetime.now()

                for docsum in root.findall("./DocSum"):
                    results.append(self._parse_docsum(docsum))
                logger.info("PMC Fetcher: processed chunk %s/%s", i + 1, len(chunks))
            except Exception as e:
                logger.error("PMC Fetcher error: %s", e)
        return results

    def _parse_docsum(self, element: ET.Element) -> Dict[str, Any]:
        # PMC esummary returns ID without prefix in <Id>
        raw_pmc = element.findtext("./Id")
        pmcid = DocumentIdentifierType.PMCID.normalize(raw_pmc)
        pmid = element.findtext("./Item[@Name='ArticleIds']/Item[@Name='pmid']")
        doi = element.findtext("./Item[@Name='ArticleIds']/Item[@Name='doi']")
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
        if not doi is None:
            identifiers[DocumentIdentifierType.DOI] = DocumentIdentifierType.DOI.normalize(doi)
        record = {
            "identifiers": identifiers,
            "journal": journal,
            "pub_year": year,
        }
        return record


class CrossrefDOIFetcher(MetadataFetcher):
    """Queries the Crossref REST API for metadata using DOIs.

    Crossref supports exact DOI lookup through ``/works/{doi}``, but for many DOIs
    it is more efficient to query the ``/works`` endpoint with repeated ``doi``
    filters. This implementation batches DOIs into modest URL-safe chunks, uses
    the Crossref polite pool when ``mailto`` is provided, and honors the
    rate-limit headers returned by Crossref.
    """

    CROSSREF_WORKS_URL = "https://api.crossref.org/works"
    DEFAULT_BATCH_SIZE = 50
    DEFAULT_WAIT_SECONDS = 1.0
    MAX_RETRIES = 3

    def __init__(
        self,
        *,
        mailto: Optional[str] = None,
        user_agent: str = "CorpusBenchmarking/0.1",
        batch_size: int = DEFAULT_BATCH_SIZE,
        wait_seconds: float = DEFAULT_WAIT_SECONDS,
        timeout: int = 30,
    ):
        self.mailto = mailto
        self.user_agent = user_agent
        self.batch_size = batch_size
        self.wait_seconds = wait_seconds
        self.timeout = timeout
        self._last_request = 0.0
        self._dynamic_wait_seconds = wait_seconds

    @property
    def supported_id_type(self) -> DocumentIdentifierType:
        return DocumentIdentifierType.DOI

    def fetch(self, dois: List[str]) -> List[Dict[str, Any]]:
        if not dois:
            return []

        normalized_dois = self._dedupe_preserve_order(DocumentIdentifierType.DOI.normalize(doi) for doi in dois if doi)

        results: List[Dict[str, Any]] = []
        chunks = [normalized_dois[i : i + self.batch_size] for i in range(0, len(normalized_dois), self.batch_size)]

        for i, chunk in enumerate(chunks):
            records = self._fetch_chunk(chunk)
            results.extend(records)
            logger.info("Crossref DOI Fetcher: processed chunk %s/%s", i + 1, len(chunks))

        return results

    def _fetch_chunk(self, dois: List[str]) -> List[Dict[str, Any]]:
        """Fetch a DOI chunk using the Crossref works endpoint.

        A query such as ``filter=doi:10.x/a,doi:10.x/b`` returns metadata for
        those exact DOIs. Missing or non-Crossref DOIs simply do not appear in
        the response.
        """
        if not dois:
            return []

        params = {
            "filter": ",".join(f"doi:{doi}" for doi in dois),
            "rows": str(len(dois)),
        }
        if self.mailto:
            params["mailto"] = self.mailto

        url = f"{self.CROSSREF_WORKS_URL}?{urllib.parse.urlencode(params)}"
        data = self._get_json_with_retries(url)
        if not data:
            return []

        items = data.get("message", {}).get("items", [])
        records = []
        seen = set()

        for item in items:
            record = self._parse_work(item)
            doi = record["identifiers"][DocumentIdentifierType.DOI]
            if doi in dois and doi not in seen:
                records.append(record)
                seen.add(doi)

        # Fallback: if Crossref ever returns fewer records than expected because
        # of filter behavior or URL length, exact /works/{doi} lookup recovers
        # the missing records without abandoning batching for the normal case.
        missing = [doi for doi in dois if doi not in seen]
        if missing and len(dois) > 1:
            for doi in missing:
                item = self._fetch_one(doi)
                if item:
                    records.append(self._parse_work(item))

        return records

    def _fetch_one(self, doi: str) -> Optional[Dict[str, Any]]:
        encoded_doi = urllib.parse.quote(doi, safe="")
        params = {}
        if self.mailto:
            params["mailto"] = self.mailto
        query = f"?{urllib.parse.urlencode(params)}" if params else ""
        url = f"{self.CROSSREF_WORKS_URL}/{encoded_doi}{query}"
        data = self._get_json_with_retries(url)
        if not data:
            return None
        return data.get("message")

    def _get_json_with_retries(self, url: str) -> Optional[Dict[str, Any]]:
        for attempt in range(self.MAX_RETRIES + 1):
            self._wait_for_rate_limit()
            req = urllib.request.Request(url, headers=self._headers())

            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    self._last_request = time.monotonic()
                    self._update_rate_limit_from_headers(response.headers)
                    return json.loads(response.read().decode("utf-8"))

            except urllib.error.HTTPError as e:
                self._last_request = time.monotonic()
                if e.code in (429, 503):
                    retry_after = self._parse_retry_after(e.headers.get("Retry-After"))
                    sleep_for = retry_after if retry_after is not None else self._backoff_seconds(attempt)
                    logger.warning(
                        "Crossref DOI Fetcher rate limited (%s); retrying after %.1fs", e.code, sleep_for
                    )
                    time.sleep(sleep_for)
                    continue

                if e.code == 404:
                    return None

                logger.error("Crossref DOI Fetcher HTTP Error %s: %s", e.code, e.reason)
                return None

            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
                if attempt >= self.MAX_RETRIES:
                    logger.error("Crossref DOI Fetcher error: %s", e)
                    return None
                sleep_for = self._backoff_seconds(attempt)
                logger.warning("Crossref DOI Fetcher transient error: %s; retrying after %.1fs", e, sleep_for)
                time.sleep(sleep_for)

        return None

    def _headers(self) -> Dict[str, str]:
        user_agent = self.user_agent
        if self.mailto and "mailto:" not in user_agent:
            user_agent = f"{user_agent} (mailto:{self.mailto})"

        return {
            "Accept": "application/json",
            "User-Agent": user_agent,
        }

    def _wait_for_rate_limit(self):
        elapsed = time.monotonic() - self._last_request
        wait_seconds = max(self.wait_seconds, self._dynamic_wait_seconds)
        if elapsed < wait_seconds:
            time.sleep(wait_seconds - elapsed)

    def _update_rate_limit_from_headers(self, headers):
        limit = headers.get("X-Rate-Limit-Limit")
        interval = headers.get("X-Rate-Limit-Interval")

        if not limit or not interval:
            return

        try:
            limit_val = int(limit)
            interval_seconds = self._parse_interval_seconds(interval)
            if limit_val > 0 and interval_seconds is not None:
                # Add a small cushion so multiple local processes or network
                # jitter do not accidentally exceed the advertised window.
                self._dynamic_wait_seconds = (interval_seconds / limit_val) * 1.10
        except ValueError:
            return

    def _parse_interval_seconds(self, interval: str) -> Optional[float]:
        match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)s\s*", interval)
        if match:
            return float(match.group(1))
        return None

    def _parse_retry_after(self, retry_after: Optional[str]) -> Optional[float]:
        if not retry_after:
            return None
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            return None

    def _backoff_seconds(self, attempt: int) -> float:
        return min(60.0, (2**attempt) * self.wait_seconds + random.uniform(0.0, 0.25))

    def _parse_work(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        doi = DocumentIdentifierType.DOI.normalize(msg.get("DOI", ""))
        identifiers = {DocumentIdentifierType.DOI: doi}

        # TODO Unclear which key is used
        pmid = self._first_assertion_value(msg, "pubmed")
        if not pmid:
            pmid = self._first_assertion_value(msg, "pmid")
        if pmid:
            identifiers[DocumentIdentifierType.PMID] = DocumentIdentifierType.PMID.normalize(pmid)

        pmcid = self._first_assertion_value(msg, "pmcid")
        if pmcid:
            identifiers[DocumentIdentifierType.PMCID] = DocumentIdentifierType.PMCID.normalize(pmcid)

        journal = self._first(msg.get("short-container-title"))
        if not journal:
            logger.debug("Crossref DOI Fetcher falling back to full journal title")
            journal = self._first(msg.get("container-title"))

        return {
            "identifiers": identifiers,
            "journal": journal,
            "pub_year": self._issued_year(msg),
        }

    def _issued_year(self, msg: Dict[str, Any]) -> Optional[str]:
        for key in ("published-print", "published-online", "published", "issued"):
            date_parts = msg.get(key, {}).get("date-parts")
            if date_parts and date_parts[0]:
                return str(date_parts[0][0])
        return None

    def _first(self, value: Any) -> Optional[Any]:
        if isinstance(value, list) and value:
            return value[0]
        return value

    def _first_assertion_value(self, msg: Dict[str, Any], name: str) -> Optional[str]:
        for assertion in msg.get("assertion", []) or []:
            if str(assertion.get("name", "")).lower() == name:
                value = assertion.get("value")
                return str(value) if value else None
        return None

    def _dedupe_preserve_order(self, values) -> List[str]:
        seen = set()
        deduped = []
        for value in values:
            if value and value not in seen:
                seen.add(value)
                deduped.append(value)
        return deduped
