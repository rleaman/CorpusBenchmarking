set -e

# Get the dataset
mkdir -p corpora/NCBI_Disease
rm -rf corpora/NCBI_Disease/*
cd corpora/NCBI_Disease
wget https://www.ncbi.nlm.nih.gov/CBBresearch/Dogan/DISEASE/NCBItrainset_corpus.zip
wget https://www.ncbi.nlm.nih.gov/CBBresearch/Dogan/DISEASE/NCBIdevelopset_corpus.zip
wget https://www.ncbi.nlm.nih.gov/CBBresearch/Dogan/DISEASE/NCBItestset_corpus.zip
unzip NCBItrainset_corpus.zip 
unzip NCBIdevelopset_corpus.zip
unzip NCBItestset_corpus.zip
rm *.zip
cd ../..

# Run 
export PYTHONPATH=src
python -m corpus_benchmark.cli configs/BC5CDR.yaml 
