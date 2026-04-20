set -e

# Get the dataset
mkdir -p corpora/NLM_Chem
rm -rf corpora/NLM_Chem/*
cd corpora/NLM_Chem
wget https://ftp.ncbi.nlm.nih.gov/pub/lu/BC7-NLM-Chem-track/BC7T2-NLMChem-corpus_v2.BioC.xml.gz
mv BC7T2-NLMChem-corpus_v2.BioC.xml BC7T2-NLMChem-corpus_v2.BioC.xml.tar
tar -xvf BC7T2-NLMChem-corpus_v2.BioC.xml.tar
rm BC7T2-NLMChem-corpus_v2.BioC.xml.tar
