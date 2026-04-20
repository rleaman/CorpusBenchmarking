# CorpusBenchmarking

**CorpusBenchmarking** is a corpus-centric Python framework for diagnosing the benchmark utility of biomedical named entity recognition (NER) and entity linking (EL) corpora. By treating corpora as measurement instruments rather than fixed inputs, the framework calculates corpus-intrinsic statistics to make explicit the constraints they place on benchmark inference.

This framework characterizes benchmarks across six diagnostic families:
* **Scale and Density**: Quantification of documents, tokens, and annotation signal.
* **Lexical and Conceptual Structure**: Measurement of mention ambiguity and surface-form variation.
* **Label Distribution**: Assessment of balance across entity type labels via Shannon entropy.
* **Overlap and Independence**: Assessment of train-test leakage risk at multiple abstraction levels.
* **Metadata Composition**: Profiling the represented literature via journal diversity and temporal coverage.
* **Ontology Coverage**: Analyzing concepts within standardized hierarchies like MeSH.

## Features

* **Registry-Based Architecture**: Easily extensible loaders for various formats and custom metrics.
* **Standardized Representation**: Converts diverse datasets into a canonical internal model of documents, passages, and annotations.
* **Format Support**: Built-in loaders for BioC XML and PubTator formats.
* **Highly Configurable**: YAML-driven configuration for individual corpora and batch "battery" analyses.
* **Advanced Filtering**: Powerful annotation filtering based on labels, link relations, and match types.
* **Efficient Processing**: Shared benchmark context with caching for expensive intermediate computations.
* **Interactive Dashboard**: Outputs structured JSON data and a self-contained HTML/JavaScript visualization dashboard.

## Install

This framework requires Python 3.11 or higher.

1.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Install the framework in editable mode**:
    ```bash
    pip install -e .
    ```

## Usage

### 1. Data Preparation
Use the provided shell scripts to download specific corpora and required terminologies:

* **Download a Corpus** (e.g., BC5CDR):
    ```bash
    bash scripts/download_BC5CDR.sh
    ```
* **Download MeSH Terminology** (Required for ontology-aware metrics):
    ```bash
    bash scripts/download_MeSH.sh
    ```

### 2. Run the Diagnostic Pipeline
To run the full suite of diagnostics (basic statistics, overlap, metadata, and terminology coverage) and update the dashboard:
```bash
bash scripts/update_output.sh
```

## Configuration Examples

The framework uses two types of YAML configurations:

### Corpus Configuration
Defines how to load a specific dataset (e.g., `configs/AnatEM.yaml`):
```yaml
name: AnatEM

loader:
  name: bioc_xml
  params:
    paths:
      train: corpora/AnatEM/trainData_allEntities.xml 
      dev: corpora/AnatEM/develData_allEntities.xml 
      test: corpora/AnatEM/testData_allEntities.xml 
    label_infon_key: type
    doc_id_map:
        pmcid: "article-id_pmc"
```

### Metrics Configuration
Defines which corpora and metrics to include in a benchmarking run (e.g., `configs/basic_corpus_stats.yaml`):
```yaml
corpora:
    AnatEM: configs/AnatEM.yaml
    BC5CDR: configs/BC5CDR.yaml

bundles:
  AnatEM_corpus:
    - corpus_name: AnatEM
      subset_name: <ALL>
  BC5CDR_corpus:
    - corpus_name: BC5CDR
      subset_name: train
    - corpus_name: BC5CDR
      subset_name: test
  
metrics:
  - metric_name: document_count
    target_bundles: ["AnatEM_corpus", "BC5CDR_corpus"]
  - metric_name: label_distribution
    target_bundles: ["AnatEM_corpus", "BC5CDR_corpus"]

output_path: "output/basic_corpus_stats.json"
```

## Implementation Notes
- Current implementation is intended to support peer review. 
    - Pending implementations include additional terminologies, downloading and loading several corpora from original formats.
- The pipeline currently supports NER-only corpora (via text- and mention-level statistics) and NER+EL corpora (via concept-level and ontology diagnostics).
- Input formats include BioC XML and PubTator. Code is extensible for additional formats.
