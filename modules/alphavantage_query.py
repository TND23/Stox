
import os

class AlphavantageQuery:
    def __init__(self, api_key, url_base):
        self.api_key = api_key
        self.target_url = url_base

    def add_function_to_url(self, function):
        self.target_url = f"{self.target_url}?function={function}"
        return self

    def add_symbol_to_url(self, symbol):
        self.target_url = f"{self.target_url}&symbol={symbol}"
        return self

    def add_interval_to_url(self, interval):
        self.target_url = f"{self.target_url}&interval={interval}"
        return self

    def add_api_key_to_url(self, api_key):
        self.target_url = f"{self.target_url}&apikey={api_key}"
        return self
    
    def print_target_url(self):
        print(self.target_url)
        return self
    