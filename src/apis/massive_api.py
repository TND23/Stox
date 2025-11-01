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
    
    def get_tickers(
        self,
        market: str = "stocks",
        active: bool = True,
        limit: int = 1000,
        sort: str = "ticker",
        order: str = "asc"
    ) -> Dict[str, Any]:
        """
        Get ticker information
        
        Args:
            market: Market type (stocks, crypto, fx)
            active: Filter for active/tradable tickers only
            limit: Results per page (max 1000)
            sort: Sort field (ticker, name, market, type, etc.)
            order: Sort order (asc, desc)
            
        Returns:
            Dictionary with ticker data and pagination info
        """
        params = {
            "market": market,
            "active": str(active).lower(),
            "limit": str(limit),
            "sort": sort,
            "order": order
        }
        
        response = self.get(
            f"{self.config.base_url}/reference/tickers",
            params=params
        )
        return response.json()
    
    def get_all_tickers(
        self,
        market: str = "stocks",
        active: bool = True,
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all tickers with automatic pagination
        
        Args:
            market: Market type (stocks, crypto, fx)
            active: Filter for active/tradable tickers only
            max_pages: Maximum number of pages to fetch (None = all)
            
        Returns:
            List of all ticker dictionaries
        """
        all_tickers = []
        page = 1
        next_url = None
        
        self.logger.info(f"Fetching all {market} tickers...")
        
        while True:
            if max_pages and page > max_pages:
                self.logger.info(f"Reached maximum pages limit: {max_pages}")
                break
            
            try:
                if next_url:
                    # Use pagination URL
                    self.logger.debug(f"Fetching page {page} (using next_url)...")
                    # Extract path from next_url
                    if next_url.startswith('http'):
                        # Full URL provided
                        response = self.get(next_url)
                    else:
                        # Relative URL
                        response = self.get(f"{self.config.base_url}{next_url}")
                else:
                    # First page
                    self.logger.debug(f"Fetching page {page}...")
                    data = self.get_tickers(market=market, active=active, limit=1000)
                    
                    results = data.get('results', [])
                    next_url = data.get('next_url')
                    
                    self.logger.info(
                        f"Page {page}: Received {len(results)} tickers "
                        f"(total so far: {len(all_tickers) + len(results)})"
                    )
                    
                    all_tickers.extend(results)
                    
                    if not next_url or len(results) == 0:
                        break
                    
                    page += 1
                    continue
                
                # Parse response for pagination
                data = response.json()
                results = data.get('results', [])
                next_url = data.get('next_url')
                
                self.logger.info(
                    f"Page {page}: Received {len(results)} tickers "
                    f"(total so far: {len(all_tickers) + len(results)})"
                )
                
                all_tickers.extend(results)
                
                if not next_url or len(results) == 0:
                    break
                
                page += 1
                
            except Exception as e:
                self.logger.error(f"Error fetching page {page}: {e}")
                break
        
        self.logger.info(f"Completed! Fetched {len(all_tickers)} total tickers")
        return all_tickers
    
    def get_ticker_details(self, ticker: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific ticker
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with detailed ticker information
        """
        try:
            response = self.get(
                f"{self.config.base_url}/reference/tickers/{ticker}"
            )
            return response.json()
        except Exception as e:
            self.logger.error(f"Error getting details for {ticker}: {e}")
            return {}
    
    def search_tickers(
        self,
        query: str,
        market: Optional[str] = None,
        active: bool = True,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search for tickers by name or symbol
        
        Args:
            query: Search query (ticker symbol or company name)
            market: Optional market filter
            active: Filter for active tickers only
            limit: Maximum results to return
            
        Returns:
            List of matching tickers
        """
        params = {
            "search": query,
            "active": str(active).lower(),
            "limit": str(limit)
        }
        
        if market:
            params["market"] = market
        
        try:
            response = self.get(
                f"{self.config.base_url}/reference/tickers",
                params=params
            )
            data = response.json()
            return data.get('results', [])
        except Exception as e:
            self.logger.error(f"Error searching for '{query}': {e}")
            return []
    
    def to_dataframe(
        self,
        tickers: List[Dict[str, Any]],
        include_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Convert ticker list to pandas DataFrame
        
        Args:
            tickers: List of ticker dictionaries
            include_columns: Optional list of columns to include
            
        Returns:
            DataFrame with ticker data
        """
        if not tickers:
            return pd.DataFrame()
        
        df = pd.DataFrame(tickers)
        
        if include_columns:
            # Only keep specified columns that exist
            existing_cols = [col for col in include_columns if col in df.columns]
            df = df[existing_cols]
        
        return df
    
    def save_tickers_to_parquet(
        self,
        filepath: str,
        market: str = "stocks",
        active: bool = True
    ) -> int:
        """
        Fetch all tickers and save to parquet file
        
        Args:
            filepath: Output file path
            market: Market type
            active: Filter for active tickers
            
        Returns:
            Number of tickers saved
        """
        tickers = self.get_all_tickers(market=market, active=active)
        
        if not tickers:
            self.logger.warning("No tickers to save")
            return 0
        
        df = self.to_dataframe(tickers)
        df.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
        
        self.logger.info(f"Saved {len(tickers)} tickers to {filepath}")
        return len(tickers)
