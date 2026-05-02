from __future__ import annotations
import logging
import re
from dataclasses import dataclass

from nltk.tokenize import sent_tokenize

from corpus_benchmark.models.types import LinkRelation, MatchType

logger = logging.getLogger(__name__)


def extract_sentences_from_texts(texts: list[str]) -> list[str]:
    sentences: list[str] = list()
    for text in texts:
        sentences.extend(sent_tokenize(text))
    return sentences


def parse_tokens(text: str) -> list[str]:
    return re.findall(r"\b\w+\b", text.replace("_", " ").lower())


def extract_tokens_from_texts(texts: list[str]) -> list[str]:
    tokens: list[str] = list()
    for text in texts:
        tokens.extend(parse_tokens(text))
    return tokens


@dataclass(slots=True)
class IdentifierFormat:
    delimiter: str
    relation: LinkRelation
    qualifier_allowed: bool


def parse_identifier_format_list(
    id_format_list: list[list[str]],
) -> list[IdentifierFormat]:
    logger.debug("Parsing %s identifier format definitions", len(id_format_list))
    if len(id_format_list) == 0:
        return []
    return [parse_identifier_format(id_format) for id_format in id_format_list]


def parse_identifier_format(id_format: list[str]) -> IdentifierFormat:

    logger.debug("Parsing identifier format %s", id_format)
    if len(id_format) != 3:
        raise ValueError()
    delimiter = id_format[0]
    relation = LinkRelation(id_format[1])
    qualifier_allowed = str_to_bool(id_format[2])
    return IdentifierFormat(delimiter, relation, qualifier_allowed)


def str_to_bool(val):
    """Convert a string representation of truth to True or False."""
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        logger.warning("Invalid boolean value encountered: %s", val)
        raise ValueError(f"Invalid boolean value: {val}")


def parse_qualifier_map(qualifier_map: dict[str, str]) -> dict[str, MatchType]:

    return {qualifier_text: MatchType(match_type_text) for qualifier_text, match_type_text in qualifier_map.items()}


def normalize_doi(value: str) -> str:
    value = str(value).strip().lower()
    # Strip common DOI resolver URLs and 'doi:' prefixes
    # Handles: https://doi.org/10... | http://dx.doi.org/10... | doi:10...
    value = re.sub(r"^(https?://)?(dx\.)?doi\.org/", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^doi:\s*", "", value, flags=re.IGNORECASE)
    return value


def normalize_pmid(value: str) -> str:
    value = str(value).strip()
    if not value.isdigit():
        raise ValueError(f"PMID should contain only digits: {value!r}")
    # Strip 'PMID:' prefixes if they somehow got included
    value = re.sub(r"^pmid:\s*", "", value, flags=re.IGNORECASE)
    return value


def normalize_pmcid(value: str) -> str:
    # Ensure it is uppercase and starts with 'PMC'
    value = str(value).strip().upper()
    if value.isdigit():
        value = f"PMC{value}"
    if not value.startswith("PMC"):
        raise ValueError(f"PMCID should look like PMC123456: {value!r}")
    return value


def normalize_issn(value: str) -> str:
    value = value.strip().upper().replace("-", "").replace(" ", "")
    if len(value) != 8:
        raise ValueError(f"ISSN should have 8 characters after normalization: {value!r}")
    return value
