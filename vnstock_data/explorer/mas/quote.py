_M='volume'
_L='Vui lòng nhập mã chứng khoán cần truy xuất khi khởi tạo Trading Class.'
_K='preparing'
_J='data_status'
_I='is_trading_hour'
_H='time'
_G='MAS.ext'
_F='records'
_E='GET'
_D='symbol'
_C=True
_B=False
_A=None
from typing import Dict,Optional,Union
from datetime import datetime
import pandas as pd
from vnai import agg_execution
from vnstock_data.explorer.mas.const import _CHART_URL,_OHLC_MAP,_INTERVAL_MAP,_OHLC_DTYPE,_RESAMPLE_MAP,_INDEX_MAPPING,_INTRADAY_MAP,_INTRADAY_DTYPE,_PRICE_DEPTH_MAP
from vnstock_data.explorer.mas.models import TickerModel
from vnstock_data.core.utils.transform import ohlc_to_df,intraday_to_df
from vnstock_data.core.utils.parser import get_asset_type
from vnstock_data.core.utils.user_agent import get_headers
from vnstock.core.utils.logger import get_logger
from vnstock_data.core.utils.client import send_request
from vnstock.core.utils.market import trading_hours
logger=get_logger(__name__)
class Quote:
	def __init__(A,symbol,random_agent=_B,show_log=_C):
		B=show_log;A.symbol=symbol.upper();A.data_source='MAS';A._history=_A;A.asset_type=get_asset_type(A.symbol);A.base_url=_CHART_URL;A.headers=get_headers(data_source=A.data_source,random_agent=random_agent);A.interval_map=_INTERVAL_MAP;A.show_log=B
		if not B:logger.setLevel('CRITICAL')
		if'INDEX'in A.symbol:A.symbol=A._index_validation()
	def _index_validation(A)->str:
		if A.symbol not in _INDEX_MAPPING.keys():raise ValueError(f"Không tìm thấy mã chứng khoán {A.symbol}. Các giá trị hợp lệ: {', '.join(_INDEX_MAPPING.keys())}")
		return _INDEX_MAPPING[A.symbol]
	def _input_validation(B,start:str,end:str,interval:str):
		A=TickerModel(symbol=B.symbol,start=start,end=end,interval=interval)
		if A.interval not in B.interval_map:raise ValueError(f"Giá trị interval không hợp lệ: {A.interval}. Vui lòng chọn: 1m, 5m, 15m, 30m, 1H, 1D, 1W, 1M")
		return A
	@agg_execution(_G)
	def history(self,start:str,end:Optional[str]=_A,interval:Optional[str]='1D',to_df:Optional[bool]=_C,show_log:Optional[bool]=_B,count_back:Optional[int]=_A,floating:Optional[int]=2)->Union[pd.DataFrame,str]:
		I='%Y-%m-%d';D=count_back;A=self;B=A._input_validation(start,end,interval);E=datetime.strptime(B.start,I)
		if end is not _A:
			F=datetime.strptime(B.end,I)+pd.Timedelta(days=1)
			if E>F:raise ValueError('Thời gian bắt đầu không thể lớn hơn thời gian kết thúc.')
			G=int(F.timestamp())
		else:G=int((datetime.now()+pd.Timedelta(days=1)).timestamp())
		J=int(E.timestamp());K=A.interval_map[B.interval];L=A.base_url+'tradingview/history';M={_D:[A.symbol],'resolution':K,'from':J,'to':G};H=send_request(url=L,headers=A.headers,method=_E,params=M,payload=_A,show_log=show_log)
		if not H:raise ValueError('Không tìm thấy dữ liệu. Vui lòng kiểm tra lại mã chứng khoán hoặc thời gian truy xuất.')
		C=ohlc_to_df(data=H,column_map=_OHLC_MAP,dtype_map=_OHLC_DTYPE,asset_type=A.asset_type,symbol=A.symbol,source=A.data_source,interval=B.interval,floating=floating,resample_map=_RESAMPLE_MAP)
		if D is not _A:C=C.tail(D)
		if to_df:return C
		else:return C.to_json(orient=_F)
	@agg_execution(_G)
	def intraday(self,page_size:Optional[int]=100,last_time:Optional[str]=_A,to_df:Optional[bool]=_C,get_all:bool=_B,show_log:bool=_B)->Union[pd.DataFrame,str]:
		D=page_size;A=self;C=trading_hours(_A)
		if C[_I]is _B and C[_J]==_K:raise ValueError(f"{C[_H]}: Dữ liệu khớp lệnh không thể truy cập trong thời gian chuẩn bị phiên mới. Vui lòng quay lại sau.")
		if A.symbol is _A:raise ValueError(_L)
		if D>30000:logger.warning('Bạn đang yêu cầu truy xuất quá nhiều dữ liệu, điều này có thể gây lỗi quá tải.')
		E=A.base_url+f"market/{A.symbol}/quote";F={_D:A.symbol,'fetchCount':D};G=send_request(url=E,headers=A.headers,method=_E,params=F,payload=_A,show_log=show_log);B=intraday_to_df(data=G['data'],column_map=_INTRADAY_MAP,dtype_map=_INTRADAY_DTYPE,symbol=A.symbol,asset_type=A.asset_type,source=A.data_source)
		if get_all:return B
		else:H=[_H,'price',_M,'match_type'];B=B[H]
		if to_df:return B
		else:return B.to_json(orient=_F)
	@agg_execution(_G)
	def price_depth(self,get_all:bool=_B,to_df:Optional[bool]=_C,show_log:Optional[bool]=_B)->Union[pd.DataFrame,str]:
		B=self;C=trading_hours(_A)
		if C[_I]is _B and C[_J]==_K:raise ValueError(f"{C[_H]}: Dữ liệu khớp lệnh không thể truy cập trong thời gian chuẩn bị phiên mới. Vui lòng quay lại sau.")
		if B.symbol is _A:raise ValueError(_L)
		D=B.base_url+'market/quoteSummary';E={_D:B.symbol};F=send_request(url=D,headers=B.headers,method=_E,params=E,payload=_A,show_log=show_log);A=pd.DataFrame(F);A=A[_PRICE_DEPTH_MAP.keys()];A.rename(columns=_PRICE_DEPTH_MAP,inplace=_C)
		if get_all==_B:G=['price',_M,'buy_volume','sell_volume','undefined_volume'];A=A[G]
		A.source=B.data_source
		if to_df:return A
		else:return A.to_json(orient=_F)