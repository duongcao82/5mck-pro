_A=False
from typing import Dict,Optional
from datetime import datetime
import requests,pandas as pd
from vnai import agg_execution
from vnstock.core.utils.parser import camel_to_snake
from vnstock.core.utils.logger import get_logger
from vnstock_data.core.utils.user_agent import get_headers
logger=get_logger(__name__)
class Listing:
	def __init__(A,random_agent:bool=_A,show_log:bool=_A):
		A.data_source='VND';A.headers=get_headers(data_source=A.data_source,random_agent=random_agent)
		if not show_log:logger.setLevel('CRITICAL')
	@agg_execution('VND.ext')
	def all_symbols(self,exchange=['HOSE','HNX','UPCOM'],show_log:Optional[bool]=_A,to_df:Optional[bool]=True)->Dict:
		G='code';F='floor';B=exchange
		if len(B)>1:B=','.join(B)
		else:B=B[0]
		E=f"https://api-finfo.vndirect.com.vn/v4/stocks?q=type:stock,ifc~floor:{B}&size=9999"
		if show_log:logger.info(f"Requested URL: {E}")
		C=requests.request('GET',E,headers=self.headers)
		if C.status_code!=200:raise ConnectionError(f"Failed to fetch data: {C.status_code} - {C.reason}")
		D=C.json();A=pd.DataFrame(D['data'])
		if F in A.columns:A=A.rename(columns={F:'exchange'})
		if G in A.columns:A=A.rename(columns={G:'symbol'})
		if to_df:
			if not D:raise ValueError('JSON data is empty or not provided.')
			A.columns=[camel_to_snake(A)for A in A.columns];A.source='VND';return A
		else:D=A.to_json(orient='records');return D