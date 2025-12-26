import requests,pandas as pd
from typing import Dict,Any
from vnstock_data.core.const import ERROR_MESSAGES
class Fetcher:
	def __init__(A,base_url:str,headers:Dict[str,str],api_key:str=None):A.base_url=base_url;A.headers=headers;A.api_key=api_key
	def fetch(A,endpoint:str,params:Dict[str,Any],extra_headers:Dict[str,str]=None)->Dict[str,Any]:
		C=extra_headers;F=f"{A.base_url}{endpoint}";D=A.headers.copy()
		if C:D.update(C)
		try:B=requests.get(F,headers=D,params=params);B.raise_for_status()
		except requests.RequestException as G:raise RuntimeError(f"{ERROR_MESSAGES["api_failure"]}: {G}")
		if B.status_code!=200:raise ValueError('No available data: Non-200 status code received.')
		E=B.json()
		if not A._has_data(E):raise ValueError('No available data: Empty response content.')
		return E
	def _has_data(B,response_data:Any)->bool:
		A=response_data
		if isinstance(A,(dict,list)):return bool(len(A))
		return False
	def validate(A,params:Dict[str,Any]):raise NotImplementedError("Override 'validate' method in the specific provider fetcher.")
	def to_dataframe(A,raw_data:Any)->pd.DataFrame:raise NotImplementedError("Override 'to_dataframe' method in the specific provider fetcher.")