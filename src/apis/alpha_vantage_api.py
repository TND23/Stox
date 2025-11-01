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
    
    def get_news_sentiment(
        self,
        tickers: Optional[str] = None,
        topics: Optional[str] = None,
        time_from: Optional[str] = None,
        time_to: Optional[str] = None,
        sort: str = "LATEST",
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Get news sentiment data
        
        Args:
            tickers: Comma-separated stock symbols (e.g., "AAPL,MSFT")
            topics: Comma-separated topics to filter by
            time_from: Start time in YYYYMMDDTHHMM format
            time_to: End time in YYYYMMDDTHHMM format
            sort: Sort order (LATEST, EARLIEST, RELEVANCE)
            limit: Number of results (max 1000)
            
        Returns:
            Dictionary with sentiment data
        """
        params = {
            "function": AlphaVantageFunction.NEWS_SENTIMENT,
            "sort": sort,
            "limit": str(limit)
        }
        
        if tickers:
            params["tickers"] = tickers
        if topics:
            params["topics"] = topics
        if time_from:
            params["time_from"] = time_from
        if time_to:
            params["time_to"] = time_to
        
        response = self.get(self.config.base_url, params=params)
        return response.json()
    
    def get_company_overview(self, symbol: str) -> Dict[str, Any]:
        """
        Get company fundamental data
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            
        Returns:
            Dictionary with company fundamentals
        """
        params = {
            "function": AlphaVantageFunction.COMPANY_OVERVIEW,
            "symbol": symbol
        }
        
        response = self.get(self.config.base_url, params=params)
        return response.json()
    
    def get_time_series_daily(
        self,
        symbol: str,
        outputsize: str = "compact"
    ) -> Dict[str, Any]:
        """
        Get daily time series data
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            outputsize: "compact" (100 days) or "full" (20+ years)
            
        Returns:
            Dictionary with daily price data
        """
        params = {
            "function": AlphaVantageFunction.TIME_SERIES_DAILY,
            "symbol": symbol,
            "outputsize": outputsize
        }
        
        response = self.get(self.config.base_url, params=params)
        return response.json()
    
    def get_time_series_intraday(
        self,
        symbol: str,
        interval: str = "5min",
        outputsize: str = "compact",
        month: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get intraday time series data
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            interval: Time interval (1min, 5min, 15min, 30min, 60min)
            outputsize: "compact" (100 points) or "full" (month of data)
            month: Optional month in YYYY-MM format for extended history
            
        Returns:
            Dictionary with intraday price data
        """
        params = {
            "function": AlphaVantageFunction.TIME_SERIES_INTRADAY,
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize
        }
        
        if month:
            params["month"] = month
        
        response = self.get(self.config.base_url, params=params)
        return response.json()
    
    def get_technical_indicator(
        self,
        function: AlphaVantageFunction,
        symbol: str,
        interval: str,
        time_period: int,
        series_type: str = "close"
    ) -> Dict[str, Any]:
        """
        Get technical indicator data
        
        Args:
            function: Technical indicator function (EMA, WMA, etc.)
            symbol: Stock symbol
            interval: Time interval (1min, 5min, 15min, 30min, 60min, daily, weekly, monthly)
            time_period: Number of data points for calculation
            series_type: Price type (close, open, high, low)
            
        Returns:
            Dictionary with technical indicator data
        """
        params = {
            "function": function,
            "symbol": symbol,
            "interval": interval,
            "time_period": str(time_period),
            "series_type": series_type
        }
        
        response = self.get(self.config.base_url, params=params)
        return response.json()
    
    def query(
        self,
        function: str,
        symbol: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Generic query method for any Alpha Vantage function
        
        Args:
            function: API function name
            symbol: Optional stock symbol
            **kwargs: Additional parameters for the API call
            
        Returns:
            Dictionary with API response
        """
        params = {"function": function}
        
        if symbol:
            params["symbol"] = symbol
        
        params.update(kwargs)
        
        response = self.get(self.config.base_url, params=params)
        return response.json()
    
    def to_dataframe(self, data: Dict[str, Any], key: Optional[str] = None) -> pd.DataFrame:
        """
        Convert API response to pandas DataFrame
        
        Args:
            data: API response dictionary
            key: Optional key to extract from nested response
            
        Returns:
            DataFrame with the data
        """
        if key and key in data:
            return pd.DataFrame(data[key])
        
        # Try to find time series data
        for k in data.keys():
            if "Time Series" in k or "Technical" in k:
                df = pd.DataFrame(data[k]).T
                df.index = pd.to_datetime(df.index)
                return df
        
        # If no time series found, convert whole response
        return pd.DataFrame(data)
