_F='VND.ext'
_E='VND'
_D='time'
_C=True
_B=None
_A=False
from typing import List,Dict,Optional
from datetime import datetime
from.const import _CHART_BASE,_INTERVAL_MAP,_OHLC_MAP,_OHLC_DTYPE,_INTRADAY_MAP,_INTRADAY_DTYPE,_INDEX_MAPPING
from vnstock.core.models import TickerModel
import requests,pandas as pd
from vnai import agg_execution
from vnstock.core.utils.parser import get_asset_type
from vnstock.core.utils.logger import get_logger
from vnstock_data.core.utils.user_agent import get_headers
logger=get_logger(__name__)
class Quote:
	def __init__(A,symbol:str,random_agent:bool=_A,show_log:bool=_A):
		A.symbol=symbol.upper();A._history=_B;A.asset_type=get_asset_type(A.symbol);A.base_url=_CHART_BASE;A.headers=get_headers(data_source=_E,random_agent=random_agent);A.interval_map=_INTERVAL_MAP;A.data_source=_E
		if not show_log:logger.setLevel('CRITICAL')
		if'INDEX'in A.symbol:A.symbol=A._index_validation()
	def _index_validation(A)->str:
		if A.symbol not in _INDEX_MAPPING.keys():raise ValueError(f"Không tìm thấy mã chứng khoán {A.symbol}. Các giá trị hợp lệ: {', '.join(_INDEX_MAPPING.keys())}")
		return _INDEX_MAPPING[A.symbol]
	def _input_validation(B,start:str,end:str,interval:str):
		A=TickerModel(symbol=B.symbol,start=start,end=end,interval=interval)
		if A.interval not in B.interval_map:raise ValueError(f"Giá trị interval không hợp lệ: {A.interval}. Vui lòng chọn: 1m, 5m, 15m, 30m, 1H, 1D, 1W, 1M")
		return A
	@agg_execution(_F)
	def history(self,start:str,end:Optional[str],interval:Optional[str]='1D',to_df:Optional[bool]=_C,show_log:Optional[bool]=_A,count_back:Optional[int]=_B)->Dict:
		K='%Y-%m-%d';H=count_back;G=show_log;E=interval;B=self;A=B._input_validation(start,end,E)
		if end is _B:I=int(datetime.now().timestamp())
		else:I=int(datetime.strptime(A.end,K).timestamp())
		L=int(datetime.strptime(A.start,K).timestamp());E=B.interval_map[A.interval];J=f"{B.base_url}/dchart/history?resolution={E}&symbol={B.symbol}&from={L}&to={I}"
		if G:logger.info(f"Tải dữ liệu từ {J}")
		D=requests.get(J,headers=B.headers)
		if D.status_code!=200:raise ConnectionError(f"Failed to fetch data: {D.status_code} - {D.reason}")
		F=D.json()
		if G:logger.info(f"Truy xuất thành công dữ liệu {A.symbol} từ {A.start} đến {A.end}, khung thời gian {A.interval}.")
		C=B._as_df(F,B.asset_type)
		if A.interval not in['1D','1W','1M']:C[_D]=C[_D]+pd.Timedelta(hours=7)
		if H is not _B:C=C.tail(H)
		if to_df:return C
		else:F=C.to_json(orient='records');return F
	def _as_df(C,history_data:Dict,asset_type:str)->pd.DataFrame:
		A=pd.DataFrame(history_data);A.drop(columns=['s'],inplace=_C);A.rename(columns=_OHLC_MAP,inplace=_C);A[_D]=pd.to_datetime(A[_D],unit='s')
		for(B,D)in _OHLC_DTYPE.items():A[B]=A[B].astype(D)
		A.attrs['name']=C.symbol;A.attrs['category']=asset_type;A.attrs['source']=_E;return A
	@agg_execution(_F)
	def intraday(self,page_size:Optional[int]=100000,to_df:Optional[bool]=_C,show_log:bool=_A)->Dict:logger.error('Dữ liệu từ VND không còn khả dụng cho Intraday. Chúng tôi đang nghiên cứu cách khắc phục.')