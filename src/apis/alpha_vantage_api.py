"""
Alpha Vantage API wrapper

Provides methods for accessing Alpha Vantage API endpoints including
sentiment data, fundamental data, and technical indicators.
"""

from enum import StrEnum
from typing import Optional, Dict, Any
import pandas as pd

from .base_api import BaseAPI


class AlphaVantageFunction(StrEnum):
    """Enum for Alpha Vantage API functions"""
    NEWS_SENTIMENT = "NEWS_SENTIMENT"
    HISTORICAL_OPTIONS = "HISTORICAL_OPTIONS"
    COMPANY_OVERVIEW = "OVERVIEW"
    TIME_SERIES_DAILY = "TIME_SERIES_DAILY"
    TIME_SERIES_INTRADAY = "TIME_SERIES_INTRADAY"
    EMA = "EMA"
    WMA = "WMA"


class AlphaVantageApi(BaseAPI):
    """
    Alpha Vantage API client
    
    Provides access to Alpha Vantage's financial data including:
    - News sentiment analysis
    - Company fundamentals
    - Historical price data
    - Technical indicators
    """
    
    def _get_api_key_params(self) -> Dict[str, str]:
        """Alpha Vantage uses 'apikey' parameter"""
        return {"apikey": self.config.api_key}
    
    def test_connection(self) -> bool:
        """
        Test API connection by fetching a simple quote
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = self.get(
                self.config.base_url,
                params={
                    "function": "TIME_SERIES_DAILY",
                    "symbol": "IBM",
                    "outputsize": "compact"
                }
            )
            data = response.json()
            return "Time Series (Daily)" in data or "Meta Data" in data
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
  