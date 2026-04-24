from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import re

NIL_RESOURCE = "<NIL>"
ALL_CORPUS_SUBSET = "<ALL>"


class MatchType(str, Enum):
    EXACT = "exact"
    RELATED = "related"
    APPROXIMATE = "approximate"
    NIL = "NIL"


class LinkRelation(str, Enum):
    DISTRIBUTIVE = "distributive"  # e.g. "endothelial and epithelial cells"
    INTERSECTIVE = "intersective"  # e.g. "inherited muscle disorder"
    RELATED_SET = "related_set"  # e.g. CellLink-style related match set
    ALTERNATIVE = "alternative"  # optional: true either/or ambiguity


@dataclass(slots=True)
class Link:
    """
    Base class for all link structures.

    This is mainly used for typing so Annotation.links can contain either
    IdentifierLink or CompositeLink.
    """

    match_type: Optional[MatchType] = None

    @abstractmethod
    def get_identifier_links(self) -> list[IdentifierLink]:
        pass


@dataclass(slots=True)
class IdentifierLink(Link):
    """
    Leaf link to a single identifier.

    identifier=None represents a NIL link.
    """

    identifier: Optional[str] = None
    resource: Optional[str] = None

    def get_identifier_links(self) -> list[IdentifierLink]:
        return [self]

    def __str__(self):
        return f"IdentifierLink(match_type = {self.match_type}, resource = {self.resource}, identifier = {self.identifier})"


NIL = IdentifierLink(identifier=None, resource=NIL_RESOURCE, match_type=MatchType.NIL)


@dataclass(slots=True)
class CompositeLink(Link):
    """
    Recursive link structure containing other links and the semantic
    relationship between them.
    """

    relation: LinkRelation = LinkRelation.RELATED_SET
    components: list[Link] = field(default_factory=list)

    def get_identifier_links(self) -> list[IdentifierLink]:
        identifier_links = list()
        for link in self.components:
            identifier_links.extend(link.get_identifier_links())
        return identifier_links

    def __str__(self):
        return f"CompositeLink(relation = {self.relation}, components = {self.components})"


@dataclass(slots=True)
class AnnotationSpan:
    start: int
    end: int


@dataclass(slots=True)
class Annotation:
    mention_id: str
    text: str
    spans: list[AnnotationSpan]
    label: Optional[str]
    link: Optional[Link]
    attributes: dict[str, str] = field(default_factory=dict)

    def get_identifier_links(self) -> list[IdentifierLink]:
        if self.link is None:
            return []
        return self.link.get_identifier_links()


@dataclass(slots=True)
class Passage:
    passage_id: str
    text: str
    offset: int
    annotations: list[Annotation] = field(default_factory=list)
    infons: dict[str, str] = field(default_factory=dict)


class DocumentIdentifierType(str, Enum):
    PMID = "pmid"
    PMCID = "pmcid"
    DOI = "doi"

    def normalize(self, value: str) -> str:
        """Standardizes the formatting of the identifier."""
        if not value:
            return value
            
        value = str(value).strip()

        if self == DocumentIdentifierType.PMCID:
            # Ensure it is uppercase and starts with 'PMC'
            value = value.upper()
            if value.startswith("PMC"):
                return value
            return f"PMC{value}"

        elif self == DocumentIdentifierType.DOI:
            # Strip common DOI resolver URLs and 'doi:' prefixes
            # Handles: https://doi.org/10... | http://dx.doi.org/10... | doi:10...
            value = re.sub(r'^(https?://)?(dx\.)?doi\.org/', '', value, flags=re.IGNORECASE)
            value = re.sub(r'^doi:\s*', '', value, flags=re.IGNORECASE)
            return value

        elif self == DocumentIdentifierType.PMID:
            # Strip accidental 'PMID:' prefixes if they somehow got included
            value = re.sub(r'^pmid:\s*', '', value, flags=re.IGNORECASE)
            return value

        # Any future types pass through stripped
        return value

@dataclass(slots=True)
class Document:
    document_id: str
    passages: list[Passage] = field(default_factory=list)
    identifiers: dict[DocumentIdentifierType, str] = field(default_factory=dict)
    infons: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CorpusSubset:
    name: str
    documents: list[Document]


@dataclass(slots=True)
class BenchmarkCorpus:
    subsets: dict[str, CorpusSubset] = field(default_factory=dict)
    metadata: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        # Add the "<ALL>" subset
        all_documents = []
        for subset in self.subsets.values():
            all_documents.extend(subset.documents)
        self.subsets[ALL_CORPUS_SUBSET] = CorpusSubset(ALL_CORPUS_SUBSET, all_documents)


@dataclass(slots=True)
class BenchmarkBattery:
    corpora: dict[str, BenchmarkCorpus] = field(default_factory=dict)
