import json
import pandas as pd
import os
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path

class DataStorage:
    """Handles data storage and retrieval for JSON and Parquet formats"""
    
    def __init__(self, base_path: str = "data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
    
    def save_json(self, data: Dict[str, Any], filename: str, subdirectory: str = "") -> str:
        """Save data as JSON file"""
        if subdirectory:
            path = self.base_path / subdirectory
            path.mkdir(exist_ok=True)
        else:
            path = self.base_path
        
        filepath = path / f"{filename}.json"
        
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, default=str)
        
        return str(filepath)
    
    def load_json(self, filename: str, subdirectory: str = "") -> Dict[str, Any]:
        """Load data from JSON file"""
        if subdirectory:
            path = self.base_path / subdirectory
        else:
            path = self.base_path
        
        filepath = path / f"{filename}.json"
        
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        with open(filepath, "r") as f:
            return json.load(f)
    
    def save_parquet(self, df: pd.DataFrame, filename: str, subdirectory: str = "") -> str:
        """Save DataFrame as Parquet file"""
        if subdirectory:
            path = self.base_path / subdirectory
            path.mkdir(exist_ok=True)
        else:
            path = self.base_path
        
        filepath = path / f"{filename}.parquet"
        df.to_parquet(filepath, index=False)
        
        return str(filepath)
    
    def load_parquet(self, filename: str, subdirectory: str = "") -> pd.DataFrame:
        """Load DataFrame from Parquet file"""
        if subdirectory:
            path = self.base_path / subdirectory
        else:
            path = self.base_path
        
        filepath = path / f"{filename}.parquet"
        
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        return pd.read_parquet(filepath)
    
    def list_files(self, subdirectory: str = "", extension: str = None) -> List[str]:
        """List files in directory"""
        if subdirectory:
            path = self.base_path / subdirectory
        else:
            path = self.base_path
        
        if not path.exists():
            return []
        
        files = []
        for file in path.iterdir():
            if file.is_file():
                if extension is None or file.suffix == f".{extension}":
                    files.append(file.name)
        
        return files
    
    def file_exists(self, filename: str, subdirectory: str = "") -> bool:
        """Check if file exists"""
        if subdirectory:
            path = self.base_path / subdirectory
        else:
            path = self.base_path
        
        filepath = path / filename
        return filepath.exists()
