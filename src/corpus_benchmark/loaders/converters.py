import shutil
from pathlib import Path
from corpus_benchmark.registry import register_converter
from corpus_benchmark.models.config import BenchmarkConfig

@register_converter("bc5cdr_converter")
def convert_bc5cdr(corpus_dir: Path, config: BenchmarkConfig):
    """
    BC5CDR extracts into a nested folder 'CDR_Data/CDR.Corpus.v010516/'.
    This moves the files to exactly where the loader expects them.
    """
    nested_dir = corpus_dir / "CDR_Data" / "CDR.Corpus.v010516"
    
    # Use the target paths defined in the config
    expected_paths = config.loader.params.get("paths", {})
    
    mapping = {
        "train": "CDR_TrainingSet.BioC.xml",
        "dev": "CDR_DevelopmentSet.BioC.xml",
        "test": "CDR_TestSet.BioC.xml",
    }

    for split, filename in mapping.items():
        src_path = nested_dir / filename
        target_path = Path(expected_paths[split])
        
        if src_path.exists() and not target_path.exists():
            print(f"    Moving {src_path.name} to target location")
            shutil.move(src_path, target_path)
            
    # Optional cleanup of extracted temp files
    if (corpus_dir / "CDR_Data").exists():
        shutil.rmtree(corpus_dir / "CDR_Data")
    if (corpus_dir / "__MACOSX").exists():
        shutil.rmtree(corpus_dir / "__MACOSX")

