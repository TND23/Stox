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

