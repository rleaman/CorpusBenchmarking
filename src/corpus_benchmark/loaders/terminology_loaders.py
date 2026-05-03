import collections
import gzip
import logging
import pathlib
import pickle
import urllib.request
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Iterator, Set, Iterable

from corpus_benchmark.models.config import WorkspaceConfig
from corpus_benchmark.models.terminologies import TerminologyResource, TerminologyConcept
from corpus_benchmark.registry import register_terminology_loader

logger = logging.getLogger(__name__)

TREETOP_NAMES: Dict[str, str] = {
    "A": "Anatomy",
    "B": "Organisms",
    "C": "Diseases",
    "D": "Chemicals and Drugs",
    "E": "Analytical, Diagnostic and Therapeutic Techniques and Equipment",
    "F": "Psychiatry and Psychology",
    "G": "Biological Sciences",
    "H": "Physical Sciences",
    "I": "Anthropology, Education, Sociology and Social Phenomena",
    "J": "Technology and Food and Beverages",
    "K": "Humanities",
    "L": "Information Science",
    "M": "Persons",
    "N": "Health Care",
    "V": "Publication Characteristics",
    "Z": "Geographic Locations",
}


def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None or elem.text is None:
        return None
    text = elem.text.strip()
    return text or None


def _normalize_mesh_ui(ui: Optional[str]) -> Optional[str]:
    if ui is None:
        return None
    normalized = ui.strip().lstrip("*").strip()
    return normalized or None


def _repair_mapped_ui_ids(resource: TerminologyResource) -> int:
    repaired = 0
    for concept in resource.concepts.values():
        normalized_ids = _unique_preserve_order(_normalize_mesh_ui(ui) for ui in concept.mapped_ui_ids)
        if normalized_ids != concept.mapped_ui_ids:
            concept.mapped_ui_ids = normalized_ids
            repaired += 1
    return repaired


def _iterparse_path_for_tag(path: pathlib.Path, tag: str) -> Iterator[ET.Element]:
    """Yield record elements one at a time using streaming parse, handling gzip if needed."""
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as fh:
            yield from _iterparse_for_tag(fh, tag)
    else:
        with open(path, "rb") as fh:
            yield from _iterparse_for_tag(fh, tag)


def _iterparse_for_tag(file_obj, tag: str) -> Iterator[ET.Element]:
    context = ET.iterparse(file_obj, events=("start", "end"))
    _, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == tag:
            yield elem
            elem.clear()
            root.clear()


def _unique_preserve_order(values: Iterable[Optional[str]]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for value in values:
        if value and value not in seen:
            out.append(value)
            seen.add(value)
    return out


def _extract_synonyms(record_el: ET.Element) -> List[str]:
    preferred_name = _text(record_el.find("DescriptorName/String")) or _text(record_el.find("QualifierName/String")) or _text(record_el.find("SupplementalRecordName/String"))

    synonyms: List[str] = []
    seen: Set[str] = set()
    for term_el in record_el.findall("ConceptList/Concept/TermList/Term"):
        term_text = _text(term_el.find("String"))
        if not term_text:
            continue
        if term_text == preferred_name:
            continue
        if term_text in seen:
            continue
        seen.add(term_text)
        synonyms.append(term_text)
    return synonyms


def _parent_tree_number(tree_number: str) -> Optional[str]:
    if "." not in tree_number:
        return None
    return tree_number.rsplit(".", 1)[0]


@register_terminology_loader("mesh_xml")
def load_mesh_xml(workspace_config: WorkspaceConfig, **params) -> TerminologyResource:
    year = params.get("year", 2026)
    name = params.get("name", f"mesh_{year}")

    terminology_dir = pathlib.Path(workspace_config.terminology_dir)
    terminology_dir.mkdir(parents=True, exist_ok=True)

    cache_path = terminology_dir / f"{name}.pkl"
    if cache_path.exists():
        logger.info(f"Loading cached terminology {name} from {cache_path}")
        with open(cache_path, "rb") as f:
            resource = pickle.load(f)
        repaired = _repair_mapped_ui_ids(resource)
        if repaired:
            logger.info("Normalized mapped MeSH UI IDs for %s cached concepts", repaired)
            with open(cache_path, "wb") as f:
                pickle.dump(resource, f)
        return resource

    logger.info(f"Building terminology {name}")

    # Check for provided paths first
    local_paths = {"descriptor": params.get("descriptor_path"), "supplemental": params.get("supplemental_path")}

    # Download files if they don't exist and paths aren't provided
    base_url = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh"

    files = {"descriptor": f"desc{year}.xml", "supplemental": f"supp{year}.xml"}

    # Mapping for gzip filenames which don't always follow a simple pattern
    gz_files = {"descriptor": f"desc{year}.gz", "supplemental": f"supp{year}.gz"}

    for key, filename in files.items():
        if local_paths[key]:
            local_paths[key] = pathlib.Path(local_paths[key])
        else:
            dest = terminology_dir / filename
            # Also check for .gz version
            gz_filename = gz_files[key]
            gz_dest = terminology_dir / gz_filename

            if not dest.exists() and not gz_dest.exists():
                # Try to download .gz first
                url = f"{base_url}/{gz_filename}"
                logger.info(f"Attempting to download {url} -> {gz_dest}")
                try:
                    # Add a basic user agent
                    opener = urllib.request.build_opener()
                    opener.addheaders = [("User-agent", "Mozilla/5.0")]
                    urllib.request.install_opener(opener)
                    urllib.request.urlretrieve(url, gz_dest)
                    local_paths[key] = gz_dest
                except Exception as e:
                    logger.warning(f"Failed to download .gz version: {e}")
                    # Fallback to plain .xml
                    url = f"{base_url}/{filename}"
                    logger.info(f"Attempting to download {url} -> {dest}")
                    try:
                        urllib.request.urlretrieve(url, dest)
                        local_paths[key] = dest
                    except Exception as e2:
                        logger.error(f"Error downloading {url}: {e2}")
                        logger.error(f"Please ensure you have internet access or provide a local path using '{key}_path' in terminology params.")
                        raise e2
            else:
                local_paths[key] = gz_dest if gz_dest.exists() else dest

    concepts: Dict[str, TerminologyConcept] = {}
    tree_to_ids: Dict[str, List[str]] = collections.defaultdict(list)

    # Parse Descriptors
    desc_path = local_paths["descriptor"]
    logger.info(f"Parsing {desc_path}")
    for record_el in _iterparse_path_for_tag(desc_path, "DescriptorRecord"):
        ui = _text(record_el.find("DescriptorUI"))
        name_val = _text(record_el.find("DescriptorName/String"))
        if not ui or not name_val:
            continue

        synonyms = _extract_synonyms(record_el)
        tree_numbers = _unique_preserve_order(_text(el) for el in record_el.findall("TreeNumberList/TreeNumber") if _text(el))
        scope_note = _text(record_el.find("ConceptList/Concept[@PreferredConceptYN='Y']/ScopeNote")) or _text(record_el.find("ConceptList/Concept/ScopeNote"))

        concepts[ui] = TerminologyConcept(ui=ui, name=name_val, synonyms=synonyms, tree_numbers=tree_numbers, scope_note=scope_note)
        for tree in tree_numbers:
            tree_to_ids[tree].append(ui)

    # Parse Supplementals
    supp_path = local_paths["supplemental"]
    logger.info(f"Parsing {supp_path}")
    for record_el in _iterparse_path_for_tag(supp_path, "SupplementalRecord"):
        ui = _text(record_el.find("SupplementalRecordUI"))
        name_val = _text(record_el.find("SupplementalRecordName/String"))
        if not ui or not name_val:
            continue

        synonyms = _extract_synonyms(record_el)
        heading_mapped_descriptor_els = record_el.findall("HeadingMappedToList/HeadingMappedTo/DescriptorReferredTo/DescriptorUI")
        mapped_descriptor_ids = _unique_preserve_order(_normalize_mesh_ui(_text(el)) for el in heading_mapped_descriptor_els)
        if not mapped_descriptor_ids:
            indexing_descriptor_els = record_el.findall("IndexingInformationList/IndexingInformation/DescriptorReferredTo/DescriptorUI")
            mapped_descriptor_ids = _unique_preserve_order(_normalize_mesh_ui(_text(el)) for el in indexing_descriptor_els)

        scope_note = _text(record_el.find("ConceptList/Concept[@PreferredConceptYN='Y']/ScopeNote")) or _text(record_el.find("ConceptList/Concept/ScopeNote"))

        concepts[ui] = TerminologyConcept(ui=ui, name=name_val, synonyms=synonyms, mapped_ui_ids=mapped_descriptor_ids, scope_note=scope_note)

    # Finalize parents
    for record in concepts.values():
        parent_ids: Set[str] = set()
        for tree in record.tree_numbers:
            parent_tree = _parent_tree_number(tree)
            if parent_tree:
                parent_ids.update(tree_to_ids.get(parent_tree, []))
        record.parent_ids = sorted(parent_ids)

    resource = TerminologyResource(name=name, concepts=concepts, tree_to_ids=dict(tree_to_ids), treetop_names=TREETOP_NAMES)

    logger.info(f"Saving terminology {name} to {cache_path}")
    with open(cache_path, "wb") as f:
        pickle.dump(resource, f)

    return resource
