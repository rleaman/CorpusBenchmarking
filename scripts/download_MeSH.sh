set -e 

# Download MeSH 2026
mkdir -p data/MeSH
rm -rf data/MeSH
# Download current MeSH XML
python -u src/mesh_tree_counts.py download --year 2026 --out-dir data/MeSH

