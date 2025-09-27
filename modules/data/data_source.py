from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import pandas as pd
import json
import os
from datetime import datetime

class DataSource(ABC):
    """Abstract base class for data sources"""
    
    @abstractmethod
    def fetch_data(self, symbol: str, **kwargs) -> Dict[str, Any]:
        """Fetch data for a given symbol"""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the data source is available"""
        pass

class AlphaVantageSource(DataSource):
    """Alpha Vantage data source implementation"""
    
    def __init__(self, api_key: str, base_url: str = "https://www.alphavantage.co/query"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = None
    
    def fetch_data(self, symbol: str, function: str = "NEWS_SENTIMENT", **kwargs) -> Dict[str, Any]:
        """Fetch data from Alpha Vantage API"""
        import requests
        
        params = {
            "function": function,
            "symbol": symbol,
            "apikey": self.api_key,
            **kwargs
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch data from Alpha Vantage: {e}")
    
    def is_available(self) -> bool:
        """Check if Alpha Vantage API is available"""
        try:
            import requests
            response = requests.get(self.base_url, timeout=5)
            return response.status_code == 200
        except:
            return False
