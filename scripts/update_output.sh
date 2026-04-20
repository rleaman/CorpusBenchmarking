set -e

# Calculate corpus metrics
python -m corpus_benchmark.cli configs/basic_corpus_stats.yaml 
python -m corpus_benchmark.cli configs/overlap_stats.yaml 
python -m corpus_benchmark.cli configs/metadata_stats.yaml 

# Get MeSH IDs for BC5CDR
grep -h "<infon key=\"MESH\">" corpora/BC5CDR/*.xml | sed "s/<infon key=\"MESH\">//g" | sed "s/<\/infon>//g" | tr "|" "\n" | sort | grep -v "-" > corpora/BC5CDR/mesh_ids.txt

# Get IDs for NCBI_Disease
cat corpora/NCBI_Disease/* | cut -sf 6 | sed "s/^[[:space:]]*//" | sed "s/[[:space:]]*$//" | sed "s/MESH://g" | tr "|" "\n" | tr "+" "\n" | sort | grep -v "OMIM:" > corpora/NCBI_Disease/mesh_ids.txt
cat corpora/NCBI_Disease/* | cut -sf 6 | sed "s/^[[:space:]]*//" | sed "s/[[:space:]]*$//" | sed "s/MESH://g" | tr "|" "\n" | tr "+" "\n" | sort | grep "OMIM:" > corpora/NCBI_Disease/omim_ids.txt

# Get IDs for NLM_Chem
cat corpora/NLM_Chem/*.xml | sed "s/^[[:space:]]*//" | sed "s/[[:space:]]*$//" | grep "<infon key=\"identifier\">" | sed "s/<infon key=\"identifier\">//g" | sed "s/<\/infon>//g" | tr "," "\n" | grep -v "-" | sed "s/MESH://g" | sort > corpora/NLM_Chem/mesh_ids.txt

# Summarize a list of MeSH IDs (one ID per line) for BC5CDR
python -u src/mesh_tree_counts.py summarize \
  --descriptor-xml data/MeSH/desc2026.xml \
  --supplemental-xml data/MeSH/supp2026.xml \
  --ids corpora/BC5CDR/mesh_ids.txt \
  --out output/BC5CDR_mesh_summary.json

# Summarize a list of MeSH IDs (one ID per line) for NCBI_Disease
python -u src/mesh_tree_counts.py summarize \
  --descriptor-xml data/MeSH/desc2026.xml \
  --supplemental-xml data/MeSH/supp2026.xml \
  --ids corpora/NCBI_Disease/mesh_ids.txt \
  --out output/NCBI_Disease_mesh_summary.json

# Summarize a list of MeSH IDs (one ID per line) for NLM_Chem
python -u src/mesh_tree_counts.py summarize \
  --descriptor-xml data/MeSH/desc2026.xml \
  --supplemental-xml data/MeSH/supp2026.xml \
  --ids corpora/NLM_Chem/mesh_ids.txt \
  --out output/NLM_Chem_mesh_summary.json

# Merge terminology statistics
python -u src/merge_json.py BC5CDR output/BC5CDR_mesh_summary.json NCBI_Disease output/NCBI_Disease_mesh_summary.json NLM_Chem output/NLM_Chem_mesh_summary.json output/terminology_coverage_stats.json

# Update dashboard
python -u src/corpus_dashboard_py.py output/basic_corpus_stats.json --overlap output/overlap_stats.json --metadata output/metadata_stats.json --output output/corpus_dashboard.html 

