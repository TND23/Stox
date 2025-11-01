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
    
    def get_top_gainers(self, count: int = 20) -> List[Dict[str, Any]]:
        """
        Get current top gaining stocks
        
        Args:
            count: Number of gainers to fetch
            
        Returns:
            List of dictionaries with gainer information
        """
        try:
            self.logger.info(f"Fetching top {count} gainers...")
            
            # Use yfinance screener
            response = yf.screen('day_gainers', count=count)
            
            gainers = []
            if response and 'quotes' in response:
                for quote in response['quotes']:
                    ticker = quote.get('symbol', '')
                    pct_change = quote.get('regularMarketChangePercent', 0)
                    volume = quote.get('regularMarketVolume', 0)
                    price = quote.get('regularMarketPrice', 0)
                    
                    if ticker and pct_change:
                        gainers.append({
                            'ticker': ticker,
                            'pct_change': round(pct_change, 2),
                            'volume': volume,
                            'current_price': price
                        })
                
                self.logger.info(f"Found {len(gainers)} top gainers")
            
            return gainers
            
        except Exception as e:
            self.logger.error(f"Error getting top gainers: {e}")
            return []
    
    def get_top_losers(self, count: int = 20) -> List[Dict[str, Any]]:
        """
        Get current top losing stocks
        
        Args:
            count: Number of losers to fetch
            
        Returns:
            List of dictionaries with loser information
        """
        try:
            self.logger.info(f"Fetching top {count} losers...")
            
            response = yf.screen('day_losers', count=count)
            
            losers = []
            if response and 'quotes' in response:
                for quote in response['quotes']:
                    ticker = quote.get('symbol', '')
                    pct_change = quote.get('regularMarketChangePercent', 0)
                    volume = quote.get('regularMarketVolume', 0)
                    price = quote.get('regularMarketPrice', 0)
                    
                    if ticker and pct_change:
                        losers.append({
                            'ticker': ticker,
                            'pct_change': round(pct_change, 2),
                            'volume': volume,
                            'current_price': price
                        })
                
                self.logger.info(f"Found {len(losers)} top losers")
            
            return losers
            
        except Exception as e:
            self.logger.error(f"Error getting top losers: {e}")
            return []
    
    def get_intraday_data(
        self,
        ticker: str,
        period: str = "5d",
        interval: str = "1m"
    ) -> Optional[pd.DataFrame]:
        """
        Get intraday price data for a ticker
        
        Args:
            ticker: Stock ticker symbol
            period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
            
        Returns:
            DataFrame with intraday data or None if failed
        """
        try:
            self.logger.info(f"Collecting {interval} data for {ticker} (period: {period})...")
            
            stock = yf.Ticker(ticker)
            df = stock.history(period=period, interval=interval)
            
            if df.empty:
                self.logger.warning(f"No data available for {ticker}")
                return None
            
            # Add ticker column
            df['ticker'] = ticker
            
            # Reset index to make datetime a column
            df.reset_index(inplace=True)
            
            # Rename columns to lowercase
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            # Calculate additional metrics
            df['price_change'] = df['close'] - df['open']
            df['price_change_pct'] = (df['price_change'] / df['open']) * 100
            df['high_low_spread'] = df['high'] - df['low']
            df['high_low_spread_pct'] = (df['high_low_spread'] / df['low']) * 100
            
            self.logger.info(f"Collected {len(df)} {interval} bars for {ticker}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error collecting data for {ticker}: {e}")
            return None
    
    def get_historical_data(
        self,
        ticker: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        period: str = "max"
    ) -> Optional[pd.DataFrame]:
        """
        Get historical daily price data
        
        Args:
            ticker: Stock ticker symbol
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)
            period: Period if start/end not specified (1d, 5d, 1mo, etc.)
            
        Returns:
            DataFrame with historical data or None if failed
        """
        try:
            stock = yf.Ticker(ticker)
            
            if start and end:
                df = stock.history(start=start, end=end)
            else:
                df = stock.history(period=period)
            
            if df.empty:
                self.logger.warning(f"No historical data for {ticker}")
                return None
            
            df['ticker'] = ticker
            df.reset_index(inplace=True)
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting historical data for {ticker}: {e}")
            return None
    
    def get_ticker_info(self, ticker: str) -> Dict[str, Any]:
        """
        Get detailed information about a ticker
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with ticker information
        """
        try:
            stock = yf.Ticker(ticker)
            return stock.info
        except Exception as e:
            self.logger.error(f"Error getting info for {ticker}: {e}")
            return {}
    
    def get_multiple_tickers(
        self,
        tickers: List[str],
        period: str = "1d",
        interval: str = "1m"
    ) -> Dict[str, pd.DataFrame]:
        """
        Get data for multiple tickers at once
        
        Args:
            tickers: List of ticker symbols
            period: Time period
            interval: Data interval
            
        Returns:
            Dictionary mapping ticker to DataFrame
        """
        results = {}
        
        for ticker in tickers:
            df = self.get_intraday_data(ticker, period, interval)
            if df is not None:
                results[ticker] = df
        
        return results
    
    def download(
        self,
        tickers: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        period: str = "max",
        interval: str = "1d",
        **kwargs
    ) -> pd.DataFrame:
        """
        Download data using yfinance.download (bulk download)
        
        Args:
            tickers: Space-separated ticker symbols (e.g., "AAPL MSFT GOOGL")
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)
            period: Period if start/end not specified
            interval: Data interval
            **kwargs: Additional arguments for yf.download
            
        Returns:
            DataFrame with data for all tickers
        """
        try:
            df = yf.download(
                tickers=tickers,
                start=start,
                end=end,
                period=period,
                interval=interval,
                **kwargs
            )
            return df
        except Exception as e:
            self.logger.error(f"Error downloading data: {e}")
            return pd.DataFrame()
