
import os
import pandas as pd
import requests
from enum import StrEnum

class AlphavantageFunction(StrEnum):
    '''
    Enum for the functions of the Alphavantage API.
    '''
    News_Sentiment = "NEWS_SENTIMENT"
    Historical_Options = "HISTORICAL_OPTIONS"
    Exp_Moving_avg = "EMA"
    Wgt_Moving_avg = "WMA"

class AlphavantageQuery:
    def __init__(self, api_key, url_base):
        '''
        Initialize the AlphavantageQuery class. Used with the Alphavantage API.
        https://www.alphavantage.co/documentation/
        Args:
            api_key: The api key for the Alphavantage API.
            url_base: The base url for the Alphavantage API.
        Returns:
            None
        '''
        self.api_key = api_key
        self.target_url = url_base
        self.function = None
        self.symbol = None
        self.interval = None
    #region url building
    def add_function_to_url(self, function: AlphavantageFunction):
        '''
        Add the function to the url.
        '''
        self.function = function
        self.target_url = f"{self.target_url}?function={function.value}"
        return self

    def add_symbol_to_url(self, symbol):
        '''
        Add the symbol to the url.
        '''
        self.symbol = symbol
        self.target_url = f"{self.target_url}&symbol={symbol}"
        return self

    def add_interval_to_url(self, interval):
        '''
        Add the interval to the url.
        '''
        self.interval = interval
        self.target_url = f"{self.target_url}&interval={interval}"
        return self

    def add_api_key_to_url(self, api_key):
        '''
        Add the api key to the url.
        '''
        self.target_url = f"{self.target_url}&apikey={api_key}"
        return self
    #endregion
    #region url modification
    def alter_url_function(self, function):
        '''
        Alter the function parameter of the url.
        '''
        self.function = function
        self.target_url = self.target_url.replace(f"function={self.function}", f"function={function}")
        return self
    
    def alter_url_symbol(self, symbol):
        '''
        Alter the symbol parameter of the url.
        '''
        self.symbol = symbol
        self.target_url = self.target_url.replace(f"symbol={self.symbol}", f"symbol={symbol}")
        return self

    def alter_url_interval(self, interval):
        '''
        Alter the interval parameter of the url.
        '''
        self.interval = interval
        self.target_url = self.target_url.replace(f"interval={self.interval}", f"interval={interval}")
        return self  
    #endregion
    
    def print_target_url(self):
        print(self.target_url)
        return self

    async def async_get_data(self):
        '''
        Get the data from the url asynchronously.
        '''
        async with aiohttp.ClientSession() as session:
            async with session.get(self.target_url) as response:
                return await response.json()

    def get_data(self):
        '''
        Get the data from the url synchronously.
        '''
        return requests.get(self.target_url).json()

    def write_to_parquet(self, path):
        df = pd.DataFrame(self.get_data())
        df.to_parquet(f"{path}/{self.symbol}.parquet")
        return self
