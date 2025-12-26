_A=None
from typing import List,Dict,Optional
from datetime import datetime,timedelta
import json,requests,pandas as pd
from vnai import agg_execution
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.parser import get_asset_type,camel_to_snake,flatten_data
from vnstock_data.core.utils.user_agent import get_headers
from vnstock_data.core.utils.validation import validate_date
from vnstock_data.core.utils.browser import get_cookie
from vnstock_data.explorer.vds.const import _BASE_URL,_ORDER_MATCH_MAPPING
logger=get_logger(__name__)
class Quote:
	def __init__(A,symbol:Optional[str],cookie=_A,random_agent=False,show_log:Optional[bool]=False):
		D='Cookie';C=show_log;B=cookie;A.symbol=symbol.upper();A.asset_type=get_asset_type(A.symbol);A.headers=get_headers(data_source='VDS',random_agent=random_agent);A.headers['Content-Type']='application/x-www-form-urlencoded; charset=UTF-8'
		if B is _A:B=get_cookie('https://livedragon.vdsc.com.vn/general/intradayBoard.rv',headers=A.headers);A.headers[D]=B
		else:A.headers[D]=B
		if not C:logger.setLevel('CRITICAL')
		A.show_log=C
	@agg_execution('VDS.ext')
	def intraday(self,date:Optional[str]=_A):
		A=self
		if date is _A:D=datetime.now()
		else:D=datetime.strptime(date,'%Y-%m-%d')
		H=D.strftime('%d/%m/%Y');E=f"{_BASE_URL}general/intradaySearch.rv";F=f"stockCode={A.symbol}&boardDate={H}"
		if A.show_log:logger.info(f"Request data to {E}, using payload as details: {F}. Headers values: {A.headers}")
		C=requests.request('POST',E,headers=A.headers,data=F)
		if C.status_code!=200:logger.debug(f"Error: {C.text}")
		G=C.json()
		if A.show_log:logger.info(G)
		B=pd.DataFrame(G['list']);B.columns=[camel_to_snake(A)for A in B.columns];B.rename(columns=_ORDER_MATCH_MAPPING,inplace=True);return B