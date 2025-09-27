from typing import Dict, Any, Optional, List
import logging
from datetime import datetime

# Handle both relative and absolute imports
try:
    from .data_source import DataSource, AlphaVantageSource
    from .data_storage import DataStorage
except ImportError:
    # Fallback for direct execution
    from data_source import DataSource, AlphaVantageSource
    from data_storage import DataStorage

class DataManager:
    """Coordinates data ingestion, storage, and retrieval operations"""
    
    def __init__(self, storage_path: str = "data", api_key: str = None):
        self.storage = DataStorage(storage_path)
        self.sources: Dict[str, DataSource] = {}
        self.logger = logging.getLogger(__name__)
        
        # Initialize default data sources
        if api_key:
            self.add_source("alpha_vantage", AlphaVantageSource(api_key))
    
    def add_source(self, name: str, source: DataSource) -> None:
        """Add a data source"""
        self.sources[name] = source
        self.logger.info(f"Added data source: {name}")
    
    def remove_source(self, name: str) -> None:
        """Remove a data source"""
        if name in self.sources:
            del self.sources[name]
            self.logger.info(f"Removed data source: {name}")
    
    def get_source(self, name: str) -> Optional[DataSource]:
        """Get a data source by name"""
        return self.sources.get(name)
    
    def list_sources(self) -> List[str]:
        """List available data sources"""
        return list(self.sources.keys())
    
    def fetch_and_store(self, symbol: str, source_name: str, function: str = "NEWS_SENTIMENT", 
                       subdirectory: str = "company_sentiment", **kwargs) -> str:
        """Fetch data from source and store it"""
        if source_name not in self.sources:
            raise ValueError(f"Data source {source_name} not found")
        
        source = self.sources[source_name]
        
        try:
            # Check if source is available
            if not source.is_available():
                raise Exception(f"Data source {source_name} is not available")
            
            # Fetch data
            self.logger.info(f"Fetching data for {symbol} from {source_name}")
            data = source.fetch_data(symbol, function=function, **kwargs)
            
            # Store data
            filename = f"{symbol}_{function}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            filepath = self.storage.save_json(data, filename, subdirectory)
            
            self.logger.info(f"Data stored at: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Failed to fetch and store data for {symbol}: {e}")
            raise
    
    def get_latest_data(self, symbol: str, subdirectory: str = "company_sentiment") -> Optional[Dict[str, Any]]:
        """Get the most recent data for a symbol"""
        try:
            files = self.storage.list_files(subdirectory, "json")
            symbol_files = [f for f in files if f.startswith(symbol)]
            
            if not symbol_files:
                return None
            
            # Sort by timestamp (assuming filename format includes timestamp)
            symbol_files.sort(reverse=True)
            latest_file = symbol_files[0].replace(".json", "")
            
            return self.storage.load_json(latest_file, subdirectory)
            
        except Exception as e:
            self.logger.error(f"Failed to get latest data for {symbol}: {e}")
            return None
    
    def get_all_data(self, symbol: str, subdirectory: str = "company_sentiment") -> List[Dict[str, Any]]:
        """Get all data for a symbol"""
        try:
            files = self.storage.list_files(subdirectory, "json")
            symbol_files = [f for f in files if f.startswith(symbol)]
            
            all_data = []
            for file in symbol_files:
                filename = file.replace(".json", "")
                data = self.storage.load_json(filename, subdirectory)
                all_data.append(data)
            
            return all_data
            
        except Exception as e:
            self.logger.error(f"Failed to get all data for {symbol}: {e}")
            return []
    
    def is_data_available(self, symbol: str, subdirectory: str = "company_sentiment") -> bool:
        """Check if data is available for a symbol"""
        files = self.storage.list_files(subdirectory, "json")
        return any(f.startswith(symbol) for f in files)

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    api_key = os.getenv('ALPHAVANTAGE_API_KEY')
    
    if api_key:
        data_manager = DataManager(api_key=api_key)
        try:
            data_manager.fetch_and_store("AAPL", "alpha_vantage", function="NEWS_SENTIMENT")
            print(data_manager.get_latest_data("AAPL"))
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("ALPHAVANTAGE_API_KEY not found in environment variables")
