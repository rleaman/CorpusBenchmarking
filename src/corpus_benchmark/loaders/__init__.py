"""Corpus loaders."""

# Import loaders so they register themselves.
from corpus_benchmark.loaders.bioc_loader import load_bioc_xml, load_pubtator
from corpus_benchmark.loaders.converters import convert_bc5cdr

__all__ = ["load_bioc_xml", "load_pubtator", "convert_bc5cdr"]
