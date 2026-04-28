set -e

# Calculate corpus metrics
python -m corpus_benchmark.cli configs/basic_corpus_stats.yaml 
python -m corpus_benchmark.cli configs/overlap_stats.yaml 
python -m corpus_benchmark.cli configs/metadata_stats.yaml 
python -m corpus_benchmark.cli configs/terminology_coverage.yaml

# Update dashboard
python src/corpus_dashboard_py.py output/basic_corpus_stats.json --overlap output/overlap_stats.json --metadata output/metadata_stats.json --terminology output/terminology_coverage_stats.json --output output/corpus_dashboard.html
