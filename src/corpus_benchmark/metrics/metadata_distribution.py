from __future__ import annotations

import datetime
import json
import os
import random
import time
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Any
from pathlib import Path

from corpus_benchmark.context import MetricTarget, get_documents
from corpus_benchmark.registry import register_subset_metric
from corpus_benchmark.results import SubsetMetricResult

default_metadata_cache_filename = "metadata/cache.json"

chunk_size = 250
wait_seconds = 0.4 # EUtils allows 3 requests per second; we wait slightly longer than 1/3 second
base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id="

def process_document_element(document_element) -> dict[str, Any]:
    pub_dict = dict()
    # PMID
    result = document_element.find("./MedlineCitation/PMID")
    pmid = result.text if not result is None else None
    pub_dict["pmid"] = pmid
    
    # PMC
    result = document_element.find("./PubmedData/ArticleIdList/ArticleId/[@IdType='pmc']")
    pmc = None
    if not result is None:
        pmc = result.text
        if not pmc.startswith("PMC"):
            pmc = "PMC" + pmc
    pub_dict["pmc"] = pmc
    
    # Journal abbreviation
    result = document_element.find("./MedlineCitation/Article/Journal/ISOAbbreviation")
    if result is None:
        result = document_element.find("./MedlineCitation/Article/Journal/Title")
    pub_dict["journal"] = result.text if not result is None else None
    
    # Publication year
    result = document_element.find("./MedlineCitation/Article/Journal/JournalIssue/PubDate/Year")
    if result is None:
        result = document_element.find("./MedlineCitation/Article/Journal/JournalIssue/PubDate/MedlineDate")
    pub_dict["pub_year"] = result.text[:4] if not result is None else None
    
    return pub_dict

def calculate_proportions(counts: Counter[str, int]) -> dict[str, float]:
    total = counts.total()
    return {
        label: (count / total if total else 0.0)
        for label, count in counts.items()
    }

def get_PubMed_metadata(pmids: list[str], metadata_cache_filename: str) -> dict[str, dict[str, Any]]:
    metadata = dict()
    
    # 1. Load existing cache from disk
    if os.path.exists(metadata_cache_filename):
        Path(metadata_cache_filename).parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(metadata_cache_filename, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Could not decode JSON from {metadata_cache_filename}. Starting fresh.")
    
    # 2. Identify PMIDs that are missing from the cache
    missing_pmids = list(set(str(pmid) for pmid in pmids if str(pmid) not in metadata))
    
    # 3. Fetch missing PMIDs via EUtils
    if missing_pmids:
        random.shuffle(missing_pmids)
        pmid_chunks = [missing_pmids[i:i + chunk_size] for i in range(0, len(missing_pmids), chunk_size)] 
        
        last_request = datetime.datetime.now() - datetime.timedelta(seconds=wait_seconds)
        new_metadata_fetched = False

        print(f"Fetching PubMed metadata for {len(missing_pmids)} missing PMIDs...")
        for index, chunk in enumerate(pmid_chunks):
            pmid_list_str = ",".join(chunk)
            url = base_url + pmid_list_str
            diff = (datetime.datetime.now() - last_request).total_seconds()
            if diff < wait_seconds:
                sleep_time = wait_seconds - diff
                time.sleep(sleep_time)
            
            try:
                with urllib.request.urlopen(url) as response:
                    xml_data = response.read()
                last_request = datetime.datetime.now()
                root = ET.fromstring(xml_data)
                
                for document_element in root.findall("./PubmedArticle"):
                    pub_dict = process_document_element(document_element)
                    if pub_dict["pmid"] is not None:
                        metadata[pub_dict["pmid"]] = pub_dict
                        new_metadata_fetched = True
                print("Request {} / {}".format(index + 1, len(pmid_chunks)))
            except Exception as e:
                print(f"Failed to fetch chunk {index + 1}: {e}")

        # 4. Write updated metadata back to disk if new records were found
        if new_metadata_fetched:
            os.makedirs(os.path.dirname(metadata_cache_filename), exist_ok=True)
            with open(metadata_cache_filename, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
                
    return metadata

def get_journals(target: MetricTarget, metadata_cache_filename: str) -> list[str]:
    documents = get_documents(target)
    pmids = [document.infons.get("pmid") for document in documents if "pmid" in document.infons]
    metadata = get_PubMed_metadata(pmids, metadata_cache_filename)
    journals = []
    for document in documents:
        pmid = document.infons.get("pmid")
        doc_meta = metadata.get(pmid, {}) if not pmid is None else {}
        journal = doc_meta.get("journal")
        journals.append(journal if journal is not None else "Unknown")
    return journals

@register_subset_metric("journal_distribution")
def journal_distribution(target: MetricTarget, result_name: str, **kwargs) -> SubsetMetricResult:
    cache_path = kwargs.get("metadata_cache_filename", default_metadata_cache_filename)
    counts = Counter(get_journals(target, cache_path))
    return SubsetMetricResult(
        result_name=result_name,
        metric_name="journal_distribution",
        value=calculate_proportions(counts),
        subset_name=target.name,
        details={
            "counts": dict(counts),
            "total": counts.total(),
        },
    )

def get_publication_years(target: MetricTarget, metadata_cache_filename: str) -> list[str]:
    documents = get_documents(target)
    pmids = [document.infons.get("pmid") for document in documents if "pmid" in document.infons]
    metadata = get_PubMed_metadata(pmids, metadata_cache_filename)
    publication_years = []
    for document in documents:
        pmid = document.infons.get("pmid")
        doc_meta = metadata.get(pmid, {}) if not pmid is None else {}
        pub_year = doc_meta.get("pub_year")
        publication_years.append(pub_year if pub_year is not None else "Unknown")
    return publication_years

@register_subset_metric("publication_year_distribution")
def publication_year_distribution(target: MetricTarget, result_name: str, **kwargs) -> SubsetMetricResult:
    cache_path = kwargs.get("metadata_cache_filename", default_metadata_cache_filename)
    counts = Counter(get_publication_years(target, cache_path))
    return SubsetMetricResult(
        result_name=result_name,
        metric_name="publication_year_distribution",
        value=calculate_proportions(counts),
        subset_name=target.name,
        details={
            "counts": dict(counts),
            "total": counts.total(),
        },
    )
