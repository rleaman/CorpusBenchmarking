#!/usr/bin/env python3
"""
Utilities for working with MeSH XML and summarizing lists of MeSH IDs.

Main capabilities:
1. Download current-or-specified-year MeSH XML files from NLM.
2. Parse Descriptor, Qualifier, and Supplemental Concept Record (SCR) XML.
3. Build a convenient in-memory representation with fields such as:
   - name
   - synonyms
   - tree_numbers
   - parent IDs
   - broader categories / branches
4. Convert a list of MeSH IDs into:
   - counts by high-level MeSH concept (treetop or first tree branch)
   - counts by MeSH tree depth

Notes:
- Current MeSH XML does not include semantic types. If you need semantic types,
  you now need a separate source such as UMLS.
- Descriptor IDs typically start with D, Supplementary Concept Record IDs with C,
  and Qualifier IDs with Q.

Standard-library only.
"""

from __future__ import annotations

import argparse
import collections
import gzip
import json
import pathlib
import sys
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import DefaultDict, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple


RECORD_LOG_FREQ = 10000

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


@dataclass
class MeshRecord:
    ui: str
    record_type: str  # descriptor | qualifier | supplemental
    name: str
    synonyms: List[str] = field(default_factory=list)
    tree_numbers: List[str] = field(default_factory=list)
    parent_ids: List[str] = field(default_factory=list)
    scope_note: Optional[str] = None
    mapped_descriptor_ids: List[str] = field(default_factory=list)  # mainly for SCRs
    semantic_types: List[str] = field(default_factory=list)  # left empty for current XML

    def to_dict(self) -> Dict[str, object]:
        return {
            "ui": self.ui,
            "record_type": self.record_type,
            "name": self.name,
            "synonyms": list(self.synonyms),
            "tree_numbers": list(self.tree_numbers),
            "parent_ids": list(self.parent_ids),
            "scope_note": self.scope_note,
            "mapped_descriptor_ids": list(self.mapped_descriptor_ids),
            "semantic_types": list(self.semantic_types),
        }


class MeshRepository:
    def __init__(self) -> None:
        self.records: Dict[str, MeshRecord] = {}
        self.tree_to_ids: DefaultDict[str, Set[str]] = collections.defaultdict(set)

    # -----------------------------
    # Construction helpers
    # -----------------------------
    @classmethod
    def from_xml(
        cls,
        descriptor_xml: str | pathlib.Path,
        qualifier_xml: str | pathlib.Path | None = None,
        supplemental_xml: str | pathlib.Path | None = None,
    ) -> "MeshRepository":
        repo = cls()
        repo._parse_descriptor_or_qualifier_xml(descriptor_xml, record_tag="DescriptorRecord", ui_tag="DescriptorUI", name_path="DescriptorName/String", record_type="descriptor")
        if qualifier_xml is not None:
            repo._parse_descriptor_or_qualifier_xml(qualifier_xml, record_tag="QualifierRecord", ui_tag="QualifierUI", name_path="QualifierName/String", record_type="qualifier")
        if supplemental_xml is not None:
            repo._parse_supplemental_xml(supplemental_xml)
        repo._finalize_parents()
        return repo

    @staticmethod
    def download_mesh_xml(year: int, out_dir: str | pathlib.Path, include_qualifiers: bool = False, include_supplementals: bool = True) -> Dict[str, pathlib.Path]:
        """Download MeSH XML files for the given production year from NLM."""
        out_path = pathlib.Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        base = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh"
        files = {
            "descriptor": f"desc{year}.xml",
        }
        if include_qualifiers:
            files["qualifier"] = f"qual{year}.xml"
        if include_supplementals:
            files["supplemental"] = f"supp{year}.xml"

        saved: Dict[str, pathlib.Path] = {}
        for key, filename in files.items():
            url = f"{base}/{filename}"
            destination = out_path / filename
            print(f"Downloading {url} -> {destination}", file=sys.stderr)
            urllib.request.urlretrieve(url, destination)
            saved[key] = destination
        return saved

    # -----------------------------
    # Parsing
    # -----------------------------
    def _parse_descriptor_or_qualifier_xml(
        self,
        xml_path: str | pathlib.Path,
        *,
        record_tag: str,
        ui_tag: str,
        name_path: str,
        record_type: str,
    ) -> None:
        print(f"Parsing {xml_path}")
        for record_index, record_el in enumerate(self._iter_record_elements(xml_path, record_tag)):
            if (record_index % RECORD_LOG_FREQ) == 0:
                print(f"\tRecord #{record_index}")
            ui = _text(record_el.find(ui_tag))
            name = _text(record_el.find(name_path))
            if not ui or not name:
                record_el.clear()
                continue

            synonyms = _extract_synonyms(record_el)
            tree_numbers = _unique_preserve_order(
                _text(el) for el in record_el.findall("TreeNumberList/TreeNumber") if _text(el)
            )
            scope_note = _text(record_el.find("ConceptList/Concept[@PreferredConceptYN='Y']/ScopeNote"))
            if not scope_note:
                # fallback if the preferred concept attribute is absent or unexpected
                scope_note = _text(record_el.find("ConceptList/Concept/ScopeNote"))

            self.records[ui] = MeshRecord(
                ui=ui,
                record_type=record_type,
                name=name,
                synonyms=synonyms,
                tree_numbers=tree_numbers,
                scope_note=scope_note,
            )

            for tree in tree_numbers:
                self.tree_to_ids[tree].add(ui)

            record_el.clear()

    def _parse_supplemental_xml(self, xml_path: str | pathlib.Path) -> None:
        print(f"Parsing {xml_path}")
        for record_index, record_el in enumerate(self._iter_record_elements(xml_path, "SupplementalRecord")):
            if (record_index % RECORD_LOG_FREQ) == 0:
                print(f"\tRecord #{record_index}")
            ui = _text(record_el.find("SupplementalRecordUI"))
            name = _text(record_el.find("SupplementalRecordName/String"))
            if not ui or not name:
                record_el.clear()
                continue

            synonyms = _extract_synonyms(record_el)
            mapped_descriptor_ids = _unique_preserve_order(
                _text(el)
                for el in record_el.findall("HeadingMappedToList/HeadingMappedTo/DescriptorReferredTo/DescriptorUI")
                if _text(el)
            )
            if not mapped_descriptor_ids:
                mapped_descriptor_ids = _unique_preserve_order(
                    _text(el)
                    for el in record_el.findall("IndexingInformationList/IndexingInformation/DescriptorReferredTo/DescriptorUI")
                    if _text(el)
                )

            scope_note = _text(record_el.find("ConceptList/Concept[@PreferredConceptYN='Y']/ScopeNote"))
            if not scope_note:
                scope_note = _text(record_el.find("ConceptList/Concept/ScopeNote"))

            self.records[ui] = MeshRecord(
                ui=ui,
                record_type="supplemental",
                name=name,
                synonyms=synonyms,
                tree_numbers=[],
                scope_note=scope_note,
                mapped_descriptor_ids=mapped_descriptor_ids,
            )
            record_el.clear()

    @staticmethod
    def _iter_record_elements(xml_path: str | pathlib.Path, record_tag: str) -> Iterator[ET.Element]:
        """Yield record elements one at a time using streaming parse."""
        path = pathlib.Path(xml_path)
        if path.suffix == ".gz":
            with gzip.open(path, "rb") as fh:
                yield from _iterparse_for_tag(fh, record_tag)
        else:
            with open(path, "rb") as fh:
                yield from _iterparse_for_tag(fh, record_tag)

    def _finalize_parents(self) -> None:
        for record in self.records.values():
            parent_ids: Set[str] = set()
            for tree in record.tree_numbers:
                parent_tree = parent_tree_number(tree)
                if parent_tree:
                    parent_ids.update(self.tree_to_ids.get(parent_tree, set()))
            record.parent_ids = sorted(parent_ids)

    # -----------------------------
    # Accessors
    # -----------------------------
    def get(self, mesh_id: str) -> Optional[MeshRecord]:
        return self.records.get(mesh_id)

    def to_jsonable(self) -> Dict[str, Dict[str, object]]:
        return {ui: record.to_dict() for ui, record in self.records.items()}

    def write_json(self, path: str | pathlib.Path) -> None:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self.to_jsonable(), fh, ensure_ascii=False, indent=2, sort_keys=True)

    # -----------------------------
    # ID resolution
    # -----------------------------
    def resolve_descriptor_records(self, mesh_id: str) -> List[MeshRecord]:
        """
        Resolve an input MeSH ID to one or more descriptor-like records that carry tree numbers.

        - Descriptor/Qualifier records resolve to themselves.
        - SCRs resolve to their mapped descriptors.
        """
        record = self.get(mesh_id)
        if record is None:
            return []
        if record.tree_numbers:
            return [record]
        resolved: List[MeshRecord] = []
        for mapped_id in record.mapped_descriptor_ids:
            mapped = self.get(mapped_id)
            if mapped is not None and mapped.tree_numbers:
                resolved.append(mapped)
        return resolved

    # -----------------------------
    # Counting
    # -----------------------------
    def count_by_treetop(self, mesh_ids: Iterable[str], count_mode: str = "unique") -> Dict[str, float]:
        counts: collections.defaultdict[str, float] = collections.defaultdict(float)
        for mesh_id in mesh_ids:
            records = self.resolve_descriptor_records(mesh_id)
            if not records:
                continue
            keys: List[str] = []
            for record in records:
                for tree in record.tree_numbers:
                    keys.append(tree[0])
            
            if not keys:
                continue
                
            if count_mode == "proportional":
                weight = 1.0 / len(keys)
                for key in keys:
                    counts[key] += weight
            elif count_mode == "all":
                for key in keys:
                    counts[key] += 1.0
            else:  # unique
                for key in set(keys):
                    counts[key] += 1.0
        return dict(sorted(counts.items()))

    def count_by_branch(self, mesh_ids: Iterable[str], count_mode: str = "unique") -> Dict[str, float]:
        """
        Count by the first tree segment, e.g. C04, D12, A11.
        This is usually the most useful 'high-level concept' summary.
        """
        counts: collections.defaultdict[str, float] = collections.defaultdict(float)
        for mesh_id in mesh_ids:
            records = self.resolve_descriptor_records(mesh_id)
            if not records:
                continue
            keys: List[str] = []
            for record in records:
                for tree in record.tree_numbers:
                    keys.append(tree.split(".")[0])
            
            if not keys:
                continue

            if count_mode == "proportional":
                weight = 1.0 / len(keys)
                for key in keys:
                    counts[key] += weight
            elif count_mode == "all":
                for key in keys:
                    counts[key] += 1.0
            else:  # unique
                for key in set(keys):
                    counts[key] += 1.0
        return dict(sorted(counts.items()))

    def count_by_depth(self, mesh_ids: Iterable[str], depth_mode: str = "unique") -> Dict[int, float]:
        """
        Count by MeSH tree depth.
        """
        counts: collections.defaultdict[int, float] = collections.defaultdict(float)
        for mesh_id in mesh_ids:
            records = self.resolve_descriptor_records(mesh_id)
            if not records:
                continue
            depths = [tree_depth(tree) for record in records for tree in record.tree_numbers]
            if not depths:
                continue

            if depth_mode == "proportional":
                weight = 1.0 / len(depths)
                for depth in depths:
                    counts[depth] += weight
            elif depth_mode == "all":
                for depth in depths:
                    counts[depth] += 1.0
            elif depth_mode == "min":
                counts[min(depths)] += 1.0
            elif depth_mode == "max":
                counts[max(depths)] += 1.0
            else:  # unique
                for depth in set(depths):
                    counts[depth] += 1.0
        return dict(sorted(counts.items()))
    
    def describe_branch_codes(self, branch_counts: Dict[str, float]) -> List[Dict[str, object]]:
        """
        Turn branch codes like C04 into a richer structure with a best-effort label.
        """
        rows: List[Dict[str, object]] = []
        for branch_code, count in sorted(branch_counts.items()):
            label = None
            candidate_ids = sorted(self.tree_to_ids.get(branch_code, []))
            if candidate_ids:
                label = self.records[candidate_ids[0]].name
            rows.append(
                {
                    "branch_code": branch_code,
                    "label": label,
                    "treetop": branch_code[0],
                    "treetop_name": TREETOP_NAMES.get(branch_code[0]),
                    "count": count,  # Now correctly handling float
                }
            )
        return rows

    def describe_treetops(self, treetop_counts: Dict[str, float]) -> List[Dict[str, object]]:
        return [
            {
                "treetop": code,
                "treetop_name": TREETOP_NAMES.get(code),
                "count": count,  # Now correctly handling float
            }
            for code, count in sorted(treetop_counts.items())
        ]

def _iterparse_for_tag(file_obj, tag: str) -> Iterator[ET.Element]:
    context = ET.iterparse(file_obj, events=("start", "end"))
    _, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == tag:
            yield elem
            elem.clear()
            root.clear()


def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None or elem.text is None:
        return None
    text = elem.text.strip()
    return text or None


def _unique_preserve_order(values: Iterable[Optional[str]]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for value in values:
        if value and value not in seen:
            out.append(value)
            seen.add(value)
    return out


def _extract_synonyms(record_el: ET.Element) -> List[str]:
    preferred_name = (
        _text(record_el.find("DescriptorName/String"))
        or _text(record_el.find("QualifierName/String"))
        or _text(record_el.find("SupplementalRecordName/String"))
    )

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


def parent_tree_number(tree_number: str) -> Optional[str]:
    if "." not in tree_number:
        return None
    return tree_number.rsplit(".", 1)[0]


def tree_depth(tree_number: str) -> int:
    return len(tree_number.split("."))


def load_mesh_ids(path: str | pathlib.Path) -> List[str]:
    ids: List[str] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            ids.append(line)
    return ids


def summarize_ids(
    repo: MeshRepository,
    mesh_ids: Sequence[str],
    *,
    high_level_mode: str = "branch",
    count_mode: str = "unique",
    depth_mode: str = "unique",
) -> Dict[str, object]:
    missing_ids = [mesh_id for mesh_id in mesh_ids if repo.get(mesh_id) is None]

    if high_level_mode == "treetop":
        high_level_counts = repo.count_by_treetop(mesh_ids, count_mode=count_mode)
        high_level_rows = repo.describe_treetops(high_level_counts)
    elif high_level_mode == "branch":
        high_level_counts = repo.count_by_branch(mesh_ids, count_mode=count_mode)
        high_level_rows = repo.describe_branch_codes(high_level_counts)
    else:
        raise ValueError(f"Unsupported high_level_mode: {high_level_mode}")

    depth_counts = repo.count_by_depth(mesh_ids, depth_mode=depth_mode)

    return {
        "n_input_ids": len(mesh_ids),
        "n_missing_ids": len(missing_ids),
        "missing_ids": missing_ids,
        "high_level_mode": high_level_mode,
        "count_mode": count_mode,
        "depth_mode": depth_mode,
        "high_level_counts": high_level_rows,
        "depth_counts": [{"depth": depth, "count": count} for depth, count in sorted(depth_counts.items())],
    }


# -----------------------------
# CLI
# -----------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Work with MeSH XML and summarize MeSH IDs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_download = subparsers.add_parser("download", help="Download MeSH XML files from NLM.")
    p_download.add_argument("--year", type=int, required=True)
    p_download.add_argument("--out-dir", required=True)
    p_download.add_argument("--include-qualifiers", action="store_true")
    p_download.add_argument("--no-supplementals", action="store_true")

    p_export = subparsers.add_parser("export-json", help="Parse XML and export a JSON dictionary of records.")
    p_export.add_argument("--descriptor-xml", required=True)
    p_export.add_argument("--qualifier-xml")
    p_export.add_argument("--supplemental-xml")
    p_export.add_argument("--out", required=True)

    p_summarize = subparsers.add_parser("summarize", help="Summarize a list of MeSH IDs by high-level branch and depth.")
    p_summarize.add_argument("--descriptor-xml", required=True) # 
    p_summarize.add_argument("--qualifier-xml")
    p_summarize.add_argument("--supplemental-xml")
    p_summarize.add_argument("--ids", required=True, help="Text file with one MeSH ID per line.")
    p_summarize.add_argument("--high-level-mode", choices=["branch", "treetop"], default="branch")
    p_summarize.add_argument("--count-mode", choices=["unique", "all", "proportional"], default="unique")
    p_summarize.add_argument("--depth-mode", choices=["unique", "all", "min", "max", "proportional"], default="unique")
    p_summarize.add_argument("--out", required=True)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.command == "download":
        files = MeshRepository.download_mesh_xml(
            year=args.year,
            out_dir=args.out_dir,
            include_qualifiers=args.include_qualifiers,
            include_supplementals=not args.no_supplementals,
        )
        print(json.dumps({k: str(v) for k, v in files.items()}, indent=2))
        return 0

    if args.command == "export-json":
        repo = MeshRepository.from_xml(
            descriptor_xml=args.descriptor_xml,
            qualifier_xml=args.qualifier_xml,
            supplemental_xml=args.supplemental_xml,
        )
        repo.write_json(args.out)
        return 0

    if args.command == "summarize":
        repo = MeshRepository.from_xml(
            descriptor_xml=args.descriptor_xml,
            qualifier_xml=args.qualifier_xml,
            supplemental_xml=args.supplemental_xml,
        )
        mesh_ids = load_mesh_ids(args.ids)
        summary = summarize_ids(
            repo,
            mesh_ids,
            high_level_mode=args.high_level_mode,
            count_mode=args.count_mode,
            depth_mode=args.depth_mode,
        )
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, ensure_ascii=False, indent=2)
        return 0

    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
