"""
Yahoo Finance API wrapper

Provides methods for accessing Yahoo Finance data including
intraday price data, top gainers/losers, and stock information.
"""

from typing import Optional, Dict, Any, List
import pandas as pd
import yfinance as yf
from datetime import datetime

from .base_api import BaseAPI


class YahooFinanceApi(BaseAPI):
    """
    Yahoo Finance API client
    
    Provides access to Yahoo Finance data including:
    - Intraday and historical price data
    - Top gainers and losers
    - Company information
    - Market data
    
    Note: Yahoo Finance doesn't require an API key but yfinance
    library is used for data access
    """
    
    def _should_add_api_key_to_params(self) -> bool:
        """Yahoo Finance doesn't use API key in params"""
        return False
    
    def test_connection(self) -> bool:
        """
        Test connection by fetching data for a known ticker
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            ticker = yf.Ticker("AAPL")
            info = ticker.info
            return 'symbol' in info or 'shortName' in info
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
   