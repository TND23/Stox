import os
from dotenv import load_dotenv
from modules.alphavantage_query import AlphavantageQuery
from constants.urls import ALPHAVANTAGE_URL_BASE

load_dotenv()
api_key = os.getenv('ALPHAVANTAGE_API_KEY')
url_base = ALPHAVANTAGE_URL_BASE
data_path = os.getenv('DATA_PATH')

if os.path.exists(data_path):
    alphavantage_query = AlphavantageQuery(api_key, url_base)
    alphavantage_query.add_function_to_url("TIME_SERIES_INTRADAY").add_symbol_to_url("IBM").add_interval_to_url("5min").add_api_key_to_url(api_key).write_to_parquet(data_path)
else:
    print('no data path found')
