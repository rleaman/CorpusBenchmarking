from __future__ import annotations

from abc import ABC, abstractmethod
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import Counter

from corpus_benchmark.models.corpus import DocumentIdentifierType

logger = logging.getLogger(__name__)

# TODO Add a registration function for the document Fetchers: which fetchers support which identifier types
# TODO Add configuration so user can specify which document fetcher to use
# TODO Make sure each fetcher documents clearly how many API calls are made per batch
# TODO Add a representation and metadata for journals
# TODO Have each document fetcher API call grab all the info it can for journals
# TODO Consider adding Fetchers for more APIs, e.g., Europe PMC
# TODO Consider adding a eUtils/esearch fetcher to support DOI lookup


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
