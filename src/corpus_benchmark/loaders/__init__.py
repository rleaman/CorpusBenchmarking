"""Corpus loaders."""

# Import loaders so they register themselves.
from corpus_benchmark.loaders.bioc_loader import load_bioc_xml

__all__ = ["load_bioc_xml"]
