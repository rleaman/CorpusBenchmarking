<<<<<<< HEAD
# CorpusBenchmarking
A framework for analyzing structural properties of biomedical NER and entity linking corpora and their impact on benchmark behavior.
=======
# corpus_benchmark_minimal

A minimal, configuration-driven Python project for benchmarking annotated corpora.

## Features

- Registry-based loaders and metrics
- Canonical internal corpus representation
- BioC XML loader using the `bioc` package
- YAML configuration
- JSON output
- Shared benchmark context for caching expensive intermediate computations

## Install

```bash
pip install -e .
```

Or, if you only want the core runtime dependencies:

```bash
pip install bioc pyyaml
```

## Example usage

```bash
python -m corpus_benchmark.cli configs/example_bioc.yaml
```

## Example config

```yaml
loader:
  name: bioc_xml
  params:
    path: sample.bioc.xml
    label_infon_key: type
    id_infon_key: identifier

metrics:
  - name: document_count
  - name: mention_count
  - name: label_distribution
```
>>>>>>> 18b8cf3 (Initial commit)
