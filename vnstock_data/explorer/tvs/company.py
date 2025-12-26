import json,pandas as pd
from typing import Dict,Optional,Union,List
from vnstock.core.utils.logger import get_logger
from vnstock_data.core.utils.user_agent import get_headers
from vnstock_data.core.utils.client import send_request
from vnstock_data.core.utils.user_agent import get_headers
from vnstock.core.utils.transform import clean_html_dict,flatten_dict_to_df,flatten_list_to_df,reorder_cols,drop_cols_by_pattern
from vnstock.core.utils.parser import get_asset_type,camel_to_snake
from vnai import agg_execution
from vnstock_data.explorer.tvs.const import _BASE_URL
import copy
logger=get_logger(__name__)
class Company:
	def __init__(A,symbol:str,random_agent:bool=False,to_df:Optional[bool]=True,show_log:Optional[bool]=False):
		B=show_log;A.symbol=symbol.upper();A.asset_type=get_asset_type(A.symbol)
		if A.asset_type not in['stock']:raise ValueError('Mã chứng khoán không hợp lệ. Chỉ cổ phiếu mới có thông tin.')
		A.headers=get_headers(data_source='TVS',random_agent=random_agent);A.show_log=B;A.to_df=to_df
		if not B:logger.setLevel('CRITICAL')
	def _fetch_data(A,url)->Dict:
		if A.show_log:logger.debug(f"Requesting data for {A.symbol} from {url}. payload: {payload}")
		B=send_request(url=url,headers=A.headers,method='POST',payload=payload,show_log=A.show_log);return B
	@agg_execution('TVS.ext')
	def overview(self)->Union[Dict,pd.DataFrame]:
		B=self;D=f"{_BASE_URL}Dashboard/GetComanyInfo?ticker={B.symbol}";C=B._fetch_data(D)
		if not C:logger.warning(f"No data available for {B.symbol}");return
		A=pd.DataFrame(C,index=[0]);A.columns=[camel_to_snake(A)for A in A.columns];A=A.rename(columns={'ticker':'symbol'})
		if B.to_df:return A
		else:return C.to_dict(orient='records')[0]if not C.empty else{}