set -e

# Get the dataset
mkdir -p corpora/BC5CDR
rm -rf corpora/BC5CDR/*
cd corpora/BC5CDR
wget https://ftp.ncbi.nlm.nih.gov/pub/lu/BC5CDR/CDR_Data.zip
unzip CDR_Data.zip
rm *.zip
mv CDR_Data/CDR.Corpus.v010516/CDR_TrainingSet.BioC.xml .
mv CDR_Data/CDR.Corpus.v010516/CDR_DevelopmentSet.BioC.xml .
mv CDR_Data/CDR.Corpus.v010516/CDR_TestSet.BioC.xml .
rm -rf CDR_Data __MACOSX 
cd ../..

# Run 
export PYTHONPATH=src
python -m corpus_benchmark.cli configs/BC5CDR.yaml 
