import datetime
import logging
import random
import re
import time
from typing import Any, Dict, List
import urllib.request
import xml.etree.ElementTree as ET

from corpus_benchmark.models.corpus import DocumentIdentifierType
from corpus_benchmark.metadata.document_metadata import DocumentMetadataFetcher

logger = logging.getLogger(__name__)

CHUNK_SIZE = 250
WAIT_SECONDS = 0.4
PUBMED_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id="
PMC_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pmc&id="

class PubMedFetcher(DocumentMetadataFetcher):
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


class PMCFetcher(DocumentMetadataFetcher):
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


