import pandas as pd
from typing import Dict,Any
from vnstock_data.core.utils.fetcher import Fetcher
from vnstock_data.core.utils.user_agent import HEADERS_MAPPING_SOURCE
from.const import BASE_URL
class SPLFetcher(Fetcher):
	def __init__(A):super().__init__(base_url=BASE_URL,headers=HEADERS_MAPPING_SOURCE.get('SIMPLIZE',{}))
	def validate(B,params:Dict[str,Any]):
		A=params
		if not A.get('ticker'):raise ValueError('Ticker is required for SPL requests.')
		if A.get('interval')not in{'1d'}:raise ValueError("Invalid interval. Only '1d' is supported.")
	def to_dataframe(D,raw_data:list)->pd.DataFrame:B='time';C=[B,'open','high','low','close','volume'];A=pd.DataFrame(raw_data,columns=C);A[B]=pd.to_datetime(A[B],unit='s');return A