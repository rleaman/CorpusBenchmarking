from __future__ import annotations

from bioc import biocxml, pubtator

from corpus_benchmark.models.corpus import (
    BenchmarkCorpus,
    CorpusSubset,
    Document,
    Passage,
    Annotation,
    AnnotationSpan,
    Link,
    IdentifierLink,
    CompositeLink,
    NIL,
    MatchType,
)
from corpus_benchmark.registry import register_loader
from corpus_benchmark.parsing import parse_identifier_format_list, parse_qualifier_map, IdentifierFormat


@register_loader("bioc_xml")
def load_bioc_xml(
    paths: dict[str, str],
    doc_id_map: dict[str, str] = {},
    passage_id_infon_key: str | None = None,
    label_infon_key: str = "type",
    id_infon_key: str = "identifier",
    label_map: dict[str, str | None] = {},
    id_format_list: list[list[str]] = [],
    qualifier_map: dict[str, str] = {},
    nil_labels: set[str] = set(),
    resource_delimiter: str = ":",
    **kwargs,
) -> BenchmarkCorpus:
    """
    Load a BioC XML file and convert it into the internal corpus model.

    Notes:
    - This minimal implementation uses only the first location for each annotation.
    - If your corpus uses discontinuous spans, extend this representation before production use.
    """
    loader = BioCXMLLoader(
        doc_id_map,
        passage_id_infon_key,
        label_infon_key,
        id_infon_key,
        label_map,
        parse_identifier_format_list(id_format_list),
        parse_qualifier_map(qualifier_map),
        nil_labels,
        resource_delimiter,
        **kwargs,
    )
    subsets = dict()
    for subset_name, subset_path in paths.items():
        subsets[subset_name] = loader.load_subset(subset_name, subset_path)
    return BenchmarkCorpus(
        subsets=subsets,
        metadata={
            "source_format": "BioC XML",
        },
    )


class Loader:

    def __init__(
        self,
        label_map: dict[str, str | None] = {},
        id_format_list: list[IdentifierFormat] = [],
        qualifier_map: dict[str, MatchType] = {},
        nil_labels: set[str] = set(),
        default_resource: str | None = None,
        resource_delimiter: str = ":",
    ):
        self.label_map = label_map
        self.id_format_list = id_format_list
        self.qualifier_map = qualifier_map
        self.nil_labels = nil_labels
        self.default_resource = default_resource
        self.resource_delimiter = resource_delimiter

    def get_label(self, label_text) -> str:
        label = self.label_map.get(label_text, label_text)
        if label is None:
            return None
        return str(label)

    def get_identifier(self, identifier_text) -> Link | None:
        if identifier_text is None:
            return None
        return self.parse_identifier(identifier_text.strip(), self.id_format_list)

    def parse_identifier(self, identifier_text: str, identifier_format_list: list[IdentifierFormat] | None) -> Link:
        # print(f"Loader.parse_identifier(): identifier_text = \"{identifier_text}\"; identifier_format_list = \"{identifier_format_list}\"")
        if identifier_format_list is None or len(identifier_format_list) == 0:
            return self.parse_atomic_identifier(identifier_text)
        identifier_format = identifier_format_list[0]
        # print(f"Loader.parse_identifier(): identifier_text = \"{identifier_text}\"; identifier_format = \"{identifier_format}\"")
        remaining_identifier_formats = identifier_format_list[1:]
        match_type = None
        # print(f"Loader.parse_identifier(): identifier_text = \"{identifier_text}\"; identifier_format.qualifier_allowed = \"{identifier_format.qualifier_allowed}\" type(identifier_format.qualifier_allowed) = \"{type(identifier_format.qualifier_allowed)}\"")
        if identifier_format.qualifier_allowed:
            #mapping_debug = [
            #    (qualifier_text, match_type, identifier_text.startswith(qualifier_text))
            #    for qualifier_text, match_type in self.qualifier_map.items()
            #]
            # print(f"Loader.parse_identifier(): identifier_text = \"{identifier_text}\"; mapping_debug = \"{mapping_debug}\"")
            mapping = [
                (len(qualifier_text), match_type)
                for qualifier_text, match_type in self.qualifier_map.items()
                if identifier_text.startswith(qualifier_text)
            ]
            if len(mapping) > 0:
                mapping.sort(reverse=True)
                match_length, match_type = mapping[0]
                identifier_text = identifier_text[match_length:]
        # print(f"Loader.parse_identifier(): identifier_text = \"{identifier_text}\"; match_type = \"{match_type}\"")
        identifier_elements = [
            self.parse_identifier(element_text.strip(), remaining_identifier_formats)
            for element_text in identifier_text.split(identifier_format.delimiter)
        ]
        # print(f"Loader.parse_identifier(): identifier_text = \"{identifier_text}\"; identifier_elements = \"{identifier_elements}\"")
        if len(identifier_elements) == 1:
            link = identifier_elements[0]
            link.match_type = match_type
            return link
        return CompositeLink(relation=identifier_format.relation, components=identifier_elements, match_type=match_type)

    def parse_atomic_identifier(self, identifier_text) -> IdentifierLink:
        # print(f"Loader.parse_atomic_identifier(): identifier_text = \"{identifier_text}\"")
        identifier_text = identifier_text.strip()
        if identifier_text in self.nil_labels:
            return NIL
        if self.resource_delimiter in identifier_text:
            fields = identifier_text.split(self.resource_delimiter)
            if len(fields) != 2:
                raise ValueError(f"Identifier \"{identifier_text}\" cannot be split into exactly 2 fields using the configured delimiter \"{self.resource_delimiter}\"")
            resource, accession = fields
        elif not self.default_resource is None:
            resource = self.default_resource
            accession = identifier_text
        else:
            resource = None
            accession = identifier_text
        return IdentifierLink(resource=resource, identifier=accession)


class BioCXMLLoader(Loader):

    def __init__(
        self,
        doc_id_map: dict[str, str] = {},
        passage_id_infon_key: str | None = None,
        label_infon_key: str = "type",
        id_infon_key: str = "identifier",
        label_map: dict[str, str | None] = {},
        id_format_list: list[IdentifierFormat] = [],
        qualifier_map: dict[str, MatchType] = {},
        nil_labels: set[str] = set(),
        resource_delimiter=":",
        **kwargs,
    ):
        super().__init__(label_map, id_format_list, qualifier_map, nil_labels, None, resource_delimiter)
        self.doc_id_map = doc_id_map
        self.passage_id_infon_key = passage_id_infon_key
        self.label_infon_key = label_infon_key
        self.id_infon_key = id_infon_key

    def load_subset(self, subset_name: str, path: str):
        """
        Load a BioC XML file and convert it into the internal corpus model.
        """
        with open(path, "r", encoding="utf-8") as fp:
            collection = biocxml.load(fp)

        # NOTE: This version silently de-duplicates documents based on the document ID
        documents: dict[str, Document] = {}

        for doc in collection.documents:
            # Copy identifiers into the doc infons
            doc_infons = {k: str(v) for k, v in doc.infons.items()}
            for id_type, location in self.doc_id_map.items():
                if location == "__DOCUMENT_ID__":
                    # Strategy 2: Document ID
                    doc_infons[id_type] = str(doc.id)
                elif len(doc.passages) > 0:
                    # Strategy 1: Specific infon key in the header (first passage)
                    val = doc.passages[0].infons.get(location)
                    if val:
                        doc_infons[id_type] = str(val)
            passages: list[Passage] = []

            for passage_index, passage in enumerate(doc.passages):
                new_passage = Passage(
                    passage_id=str(passage.infons.get(self.passage_id_infon_key, passage_index)),
                    text=passage.text,
                    offset=int(passage.offset),
                    annotations=[],
                    infons=doc_infons,
                )
                for ann in passage.annotations:
                    mention = self.get_mention(ann)
                    if not mention is None:
                        new_passage.annotations.append(mention)
                passages.append(new_passage)

            document_id = str(doc.id)
            documents[document_id] = Document(
                document_id=str(doc.id),
                passages=passages,
                infons=doc_infons,
            )
        return CorpusSubset(name=subset_name, documents=list(documents.values()))

    def get_mention(self, ann):
        spans = []
        for location in ann.locations:
            span = AnnotationSpan(
                start=int(location.offset),
                end=int(location.offset) + int(location.length),
            )
            spans.append(span)

        label = self.get_label(ann.infons.get(self.label_infon_key))
        if label is None:
            return None

        mention = Annotation(
            mention_id=str(ann.id),
            text=ann.text,
            spans=spans,
            label=label,
            link=self.get_identifier(ann.infons.get(self.id_infon_key)),
            attributes={k: str(v) for k, v in ann.infons.items()},
        )

        return mention


@register_loader("pubtator")
def load_pubtator(
    paths: dict[str, str],
    label_map: dict[str, str | None] = {},
    id_format_list: list[list[str]] = [],
    qualifier_map: dict[str, str] = {},
    nil_labels: set[str] = set(),
    default_resource: str | None = None,
    resource_delimiter: str = ":",
) -> BenchmarkCorpus:
    """
    Load a BioC XML file and convert it into the internal corpus model.

    Notes:
    - This minimal implementation uses only the first location for each annotation.
    - If your corpus uses discontinuous spans, extend this representation before production use.
    """
    loader = BioCPubtatorLoader(
        label_map,
        parse_identifier_format_list(id_format_list),
        parse_qualifier_map(qualifier_map),
        nil_labels,
        default_resource,
        resource_delimiter,
    )
    subsets = dict()
    for subset_name, subset_path in paths.items():
        subsets[subset_name] = loader.load_subset(subset_name, subset_path)
    return BenchmarkCorpus(
        subsets=subsets,
        metadata={
            "source_format": "Pubtator",
        },
    )


class BioCPubtatorLoader(Loader):

    def __init__(
        self,
        label_map: dict[str, str | None] = {},
        id_format_list: list[IdentifierFormat] = [],
        qualifier_map: dict[str, MatchType] = {},
        nil_labels: set[str] = set(),
        default_resource: str | None = None,
        resource_delimiter: str = ":",
    ):
        super().__init__(label_map, id_format_list, qualifier_map, nil_labels, default_resource, resource_delimiter)

    def load_subset(self, subset_name: str, path: str):
        """
        Load a BioC Pubtator file and convert it into the internal corpus model.
        """
        with open(path, "r", encoding="utf-8") as fp:
            collection = pubtator.load(fp)

        documents: list[Document] = []

        for doc in collection:
            pmid = str(doc.pmid)
            title_passage = Passage(
                passage_id=f"{pmid}_t",
                text=doc.title,
                offset=0,
                annotations=[],
            )
            abstract_offset = len(doc.title) + 1
            abstract_passage = Passage(
                passage_id=f"{pmid}_a",
                text=doc.text,
                offset=abstract_offset,
                annotations=[],
            )
            for ann in doc.annotations:
                label = self.get_label(ann.type)
                if label is None:
                    continue
                mention = Annotation(
                    mention_id=str(ann.id),
                    text=ann.text,
                    spans=[
                        AnnotationSpan(
                            start=ann.start,
                            end=ann.end,
                        )
                    ],
                    label=label,
                    link=self.get_identifier(ann.id),
                )
                if ann.start >= abstract_offset:
                    title_passage.annotations.append(mention)
                else:
                    abstract_passage.annotations.append(mention)
            documents.append(
                Document(
                    document_id=pmid,
                    passages=[title_passage, abstract_passage],
                    infons={"pmid": pmid}
                )
            )
        return CorpusSubset(name=subset_name, documents=documents)
