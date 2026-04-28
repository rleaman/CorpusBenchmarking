from __future__ import annotations

import tarfile
import urllib.request
import zipfile
from pathlib import Path

from corpus_benchmark.models.config import BenchmarkConfig, WorkspaceConfig
from corpus_benchmark.registry import CONVERTERS

class AcquisitionManager:
    def __init__(self, workspace_config: WorkspaceConfig):
        self.download_dir = Path(workspace_config.corpora_download_dir)

    def ensure_corpus_ready(self, corpus_name: str, config: BenchmarkConfig) -> None:
        """Ensures that the required corpus files exist locally, acquiring them if necessary."""
        # 1. Check if all target paths already exist
        paths = config.loader.params.get("paths", {})
        all_exist = True
        for split, path in paths.items():
            if not Path(path).exists():
                all_exist = False
                break
        
        if all_exist:
            return  # Corpus is already ready!

        if not config.acquisition:
            raise FileNotFoundError(
                f"Files for corpus '{corpus_name}' are missing, and no acquisition spec was provided."
            )

        print(f"Acquiring corpus '{corpus_name}'...")
        corpus_dir = self.download_dir / corpus_name
        corpus_dir.mkdir(parents=True, exist_ok=True)

        # 2. Download files
        downloaded_files = []
        for url in config.acquisition.source_urls:
            filename = url.split("/")[-1]
            dest_path = corpus_dir / filename
            if not dest_path.exists():
                print(f"  Downloading {url} -> {dest_path}")
                # For robust implementation, consider adding headers or handling redirects here
                urllib.request.urlretrieve(url, dest_path)
            downloaded_files.append(dest_path)

        # 3. Extract archives if specified
        fmt = config.acquisition.format
        if fmt == "zip":
            for file_path in downloaded_files:
                print(f"  Extracting {file_path}")
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(corpus_dir)
        elif fmt in ["tar", "tar.gz", "tgz"]:
            for file_path in downloaded_files:
                print(f"  Extracting {file_path}")
                with tarfile.open(file_path, 'r:*') as tar_ref:
                    tar_ref.extractall(corpus_dir)

        # 4. Run custom converter/mapper if specified
        if config.acquisition.converter:
            converter_func = CONVERTERS.get(config.acquisition.converter)
            if not converter_func:
                raise ValueError(f"Converter '{config.acquisition.converter}' not found in registry.")
            print(f"  Running converter '{config.acquisition.converter}'")
            converter_func(corpus_dir, config)

        # 5. Final validation: Check if paths exist after acquisition is complete
        for split, path in paths.items():
            if not Path(path).exists():
                raise FileNotFoundError(
                    f"After acquisition, expected file '{path}' for split '{split}' is still missing!"
                )
        print(f"  Corpus '{corpus_name}' acquired and content verified")


