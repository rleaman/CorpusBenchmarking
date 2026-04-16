# CorpusBenchmarking

A Python framework for analyzing structural properties of biomedical NER and entity linking corpora and their impact on benchmark behavior.

## Features

- Registry-based loaders and metrics
- Canonical internal corpus representation
- BioC XML loader using the `bioc` package
- YAML configuration
- JSON output
- Shared benchmark context for caching expensive intermediate computations

## Install

```bash
pip install -r requirements.txt
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
