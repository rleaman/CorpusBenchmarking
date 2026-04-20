set -e

# Get the dataset
mkdir -p corpora/CHEMDNER
rm -rf corpora/CHEMDNER/*
cd corpora/CHEMDNER
wget https://ftp.ncbi.nlm.nih.gov/pub/lu/BC7-NLM-Chem-track/BC7T2-CHEMDNER-corpus_v2.BioC.xml.gz
mv BC7T2-CHEMDNER-corpus_v2.BioC.xml.gz BC7T2-CHEMDNER-corpus_v2.BioC.xml.tar
tar -xvf BC7T2-CHEMDNER-corpus_v2.BioC.xml.tar 
rm BC7T2-CHEMDNER-corpus_v2.BioC.xml.tar
