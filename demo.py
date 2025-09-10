import os
import requests
from dotenv import load_dotenv
from modules.alphavantage_query import AlphavantageQuery


load_dotenv()
api_key = os.getenv('ALPHAVANTAGE_API_KEY')
url_base = os.getenv('ALPHAVANTAGE_URL_BASE')

alphavantage_query = AlphavantageQuery(api_key, url_base)
target_url = alphavantage_query.add_function_to_url("TIME_SERIES_INTRADAY").add_symbol_to_url("IBM").add_interval_to_url("5min").add_api_key_to_url(api_key)
alphavantage_query.print_target_url()

r = requests.get(target_url.target_url)
data = r.json()
print(data)

