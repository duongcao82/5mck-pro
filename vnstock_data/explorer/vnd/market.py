_B='VNINDEX'
_A='VND.ext'
import requests,pandas as pd
from vnai import agg_execution
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.user_agent import get_headers
from vnstock_data.core.utils.parser import lookback_date
from vnstock_data.explorer.vnd.const import _INDEX_MAPPING
logger=get_logger(__name__)
class Market:
	def __init__(A,index:str=_B,random_agent:bool=False,show_log=False):
		A.index=A._index_validation(index);A.base_url='https://api-finfo.vndirect.com.vn/v4/ratios';A.headers=get_headers(data_source='VND',random_agent=random_agent)
		if not show_log:logger.setLevel('CRITICAL')
	def _index_validation(C,index:str)->str:
		B='VN30';A=index;A=A.upper()
		if A not in[_B,'HNXINDEX',B]:raise ValueError(f"Invalid index: {A}. Valid indices are: 'VNINDEX', 'HNX'")
		if A==B:return B
		else:return _INDEX_MAPPING[A]
	def _fetch_data(B,ratio_code:str,start_date:str)->pd.DataFrame:
		D='reportDate';C=ratio_code;G=f"{B.base_url}?q=ratioCode:{C}~code:{B.index}~reportDate:gte:{start_date}&sort=reportDate:desc&size=10000&fields=value,reportDate"
		try:
			logger.info(f"Fetching {C} data for index {B.index}...");E=requests.get(G,headers=B.headers);E.raise_for_status();F=E.json().get('data',[])
			if not F:logger.warning('No data returned from API.');return pd.DataFrame()
			A=pd.DataFrame(F);A[D]=pd.to_datetime(A[D]);A=A.rename(columns={'value':C.lower()});A=A.rename(columns={'price_to_earnings':'pe','price_to_book':'pb'});return A.set_index(D).sort_index()
		except requests.RequestException as H:logger.error(f"Failed to fetch data: {H}");return pd.DataFrame()
	@agg_execution(_A)
	def pe(self,duration:str='5Y')->pd.DataFrame:A=lookback_date(duration);return self._fetch_data(ratio_code='PRICE_TO_EARNINGS',start_date=A)
	@agg_execution(_A)
	def pb(self,duration:str='5Y')->pd.DataFrame:A=lookback_date(duration);return self._fetch_data(ratio_code='PRICE_TO_BOOK',start_date=A)
	@agg_execution(_A)
	def evaluation(self,duration:str='5Y')->pd.DataFrame:
		A=duration;E=lookback_date(A);B=self.pe(duration=A);C=self.pb(duration=A)
		if B.empty and C.empty:logger.warning('No data available for both P/E and P/B ratios.');return pd.DataFrame()
		D=pd.concat([B,C],axis=1);return D