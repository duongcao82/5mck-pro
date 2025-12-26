_F='%Y-%m-%d'
_E="Không có trường 'data' trong phản hồi JSON"
_D='data'
_C='VNINDEX'
_B=None
_A='VND.ext'
import pandas as pd,requests
from typing import Union
from datetime import datetime
from vnai import agg_execution
from vnstock_data.explorer.vnd.const import _INSIGHT_BASE,_TOP_STOCK_INDEX,_TOP_STOCK_COLS
from vnstock.core.utils.logger import get_logger
from vnstock_data.core.utils.user_agent import get_headers
logger=get_logger(__name__)
class TopStock:
	def __init__(A,show_log:bool=False,random_agent:bool=False):
		C='VND';B=show_log;A.show_log=B;A.base_url=_INSIGHT_BASE;A.headers=get_headers(data_source=C,random_agent=random_agent);A.data_source=C
		if not B:logger.setLevel('CRITICAL')
	def _fetch_data(A,url:str)->pd.DataFrame:
		try:
			if A.show_log:logger.info(f"Lấy dữ liệu từ URL: {url}")
			B=requests.get(url,headers=A.headers);B.raise_for_status();C=B.json()
			if _D in C:D=pd.DataFrame(C[_D]);D.rename(columns=_TOP_STOCK_COLS,inplace=True);return D
			else:logger.error(_E);return pd.DataFrame()
		except requests.exceptions.RequestException as E:logger.error(f"Lỗi khi lấy dữ liệu: {E}");return pd.DataFrame()
	def _fetch_foreign_data(A,url:str)->pd.DataFrame:
		try:
			if A.show_log:logger.info(f"Lấy dữ liệu từ URL: {url}")
			B=requests.get(url,headers=A.headers);B.raise_for_status();C=B.json()
			if _D in C:D=pd.DataFrame(C[_D]);D.rename(columns={'code':'symbol','tradingDate':'date','netVal':'net_value'},inplace=True);return D
			else:logger.error(_E);return pd.DataFrame()
		except requests.exceptions.RequestException as E:logger.error(f"Lỗi khi lấy dữ liệu: {E}");return pd.DataFrame()
	def _get_index_code(A,index:str)->str:return _TOP_STOCK_INDEX.get(index.upper(),'VNIndex')
	@agg_execution(_A)
	def gainer(self,index:str=_C,limit:int=10)->pd.DataFrame:A=self;B=A._get_index_code(index);C=f"{A.base_url}/top_stocks?q=index:{B}~nmVolumeAvgCr20D:gte:10000~priceChgPctCr1D:gt:0&size={limit}&sort=priceChgPctCr1D";return A._fetch_data(C)
	@agg_execution(_A)
	def loser(self,index:str=_C,limit:int=10)->pd.DataFrame:A=self;B=A._get_index_code(index);C=f"{A.base_url}/top_stocks?q=index:{B}~nmVolumeAvgCr20D:gte:10000~priceChgPctCr1D:lt:0&size={limit}&sort=priceChgPctCr1D:asc";return A._fetch_data(C)
	@agg_execution(_A)
	def value(self,index:str=_C,limit:int=10)->pd.DataFrame:A=self;B=A._get_index_code(index);C=f"{A.base_url}/top_stocks?q=index:{B}~accumulatedVal:gt:0&size={limit}&sort=accumulatedVal";return A._fetch_data(C)
	@agg_execution(_A)
	def volume(self,index:str=_C,limit:int=10)->pd.DataFrame:A=self;B=A._get_index_code(index);C=f"{A.base_url}/top_stocks?q=index:{B}~nmVolumeAvgCr20D:gte:10000~nmVolNmVolAvg20DPctCr:gte:100&size={limit}&sort=nmVolNmVolAvg20DPctCr";return A._fetch_data(C)
	@agg_execution(_A)
	def deal(self,index:str=_C,limit:int=10)->pd.DataFrame:A=self;B=A._get_index_code(index);C=f"{A.base_url}/top_stocks?size={limit}&q=index:{B}~nmVolumeAvgCr20D:gte:10000&sort=ptVolTotalVolAvg20DPctCr";return A._fetch_data(C)
	@agg_execution(_A)
	def foreign_buy(self,date:Union[str,_B]=_B,limit:int=10)->pd.DataFrame:
		A=date
		if A is _B:A=datetime.now().strftime(_F)
		B=f"https://api-finfo.vndirect.com.vn/v4/foreigns?q=type:STOCK,IFC,ETF~netVal:gt:0~tradingDate:{A}&sort=tradingDate~netVal:desc&size={limit}&fields=code,netVal,tradingDate";return self._fetch_foreign_data(B)
	@agg_execution(_A)
	def foreign_sell(self,date:Union[str,_B]=_B,limit:int=10)->pd.DataFrame:
		A=date
		if A is _B:A=datetime.now().strftime(_F)
		B=f"https://api-finfo.vndirect.com.vn/v4/foreigns?q=type:STOCK,IFC,ETF~netVal:lt:0~tradingDate:{A}&sort=tradingDate~netVal:asc&size={limit}&fields=code,netVal,tradingDate";return self._fetch_foreign_data(B)