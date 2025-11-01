"""
Massive.com API wrapper

Provides methods for accessing Massive.com reference data including
US stock tickers, company information, and market data.
"""

from typing import Optional, Dict, Any, List
import pandas as pd

from .base_api import BaseAPI


class MassiveApi(BaseAPI):
    """
    Massive.com API client
    
    Provides access to Massive.com reference data including:
    - US stock tickers
    - Company information
    - Market reference data
    """
    
    def _get_api_key_params(self) -> Dict[str, str]:
        """Massive uses 'apiKey' parameter"""
        return {"apiKey": self.config.api_key}
    
    def test_connection(self) -> bool:
        """
        Test API connection by fetching a small amount of data
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = self.get(
                f"{self.config.base_url}/reference/tickers",
                params={
                    "market": "stocks",
                    "active": "true",
                    "limit": "1"
                }
            )
            data = response.json()
            return "results" in data
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
 