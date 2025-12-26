_L='symbol'
_K='Vui lòng nhập mã chứng khoán cần truy xuất khi khởi tạo Trading Class.'
_J='preparing'
_I='data_status'
_H='is_trading_hour'
_G='VCI.ext'
_F='records'
_E='time'
_D='POST'
_C=True
_B=False
_A=None
import pandas as pd
from datetime import datetime
from vnai import agg_execution
from typing import Dict,Optional,Union
from vnstock.explorer.vci.const import _TRADING_URL,_INTERVAL_MAP,_OHLC_MAP,_RESAMPLE_MAP,_OHLC_DTYPE,_INTRADAY_URL,_INTRADAY_MAP,_INTRADAY_DTYPE,_INDEX_MAPPING
from vnstock_data.explorer.vci.const import _PRICE_DEPTH_MAP
from vnstock.core.models import TickerModel
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.market import trading_hours
from vnstock.core.utils.parser import get_asset_type
from vnstock_data.core.utils.client import send_request,ProxyConfig
from vnstock_data.core.utils.user_agent import get_headers
from vnstock_data.core.utils.transform import ohlc_to_df,intraday_to_df
logger=get_logger(__name__)
class Quote:
	def __init__(A,symbol,random_agent=_B,proxy_config:Optional[ProxyConfig]=_A,show_log=_C):
		C=show_log;B=proxy_config;A.symbol=symbol.upper();A.data_source='VCI';A._history=_A;A.asset_type=get_asset_type(A.symbol);A.base_url=_TRADING_URL;A.headers=get_headers(data_source=A.data_source,random_agent=random_agent);A.interval_map=_INTERVAL_MAP;A.show_log=C;A.proxy_config=B if B is not _A else ProxyConfig()
		if not C:logger.setLevel('CRITICAL')
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
		N='%Y-%m-%d';G=end;C=count_back;A=self;F=A._input_validation(start,G,interval);H=datetime.strptime(F.start,N)
		if G is not _A:
			D=datetime.strptime(F.end,N)+pd.Timedelta(days=1)
			if H>D:raise ValueError('Thời gian bắt đầu không thể lớn hơn thời gian kết thúc.')
			K=int(D.timestamp())
		else:D=datetime.now()+pd.Timedelta(days=1);K=int(D.timestamp())
		L=A.interval_map[F.interval];E=1000;I=pd.bdate_range(start=H,end=D)
		if C is _A and G is not _A:
			J=L
			if J=='ONE_DAY':E=len(I)+1
			elif J=='ONE_HOUR':E=len(I)*6.5+1
			elif J=='ONE_MINUTE':E=len(I)*6.5*60+1
		else:E=C if C is not _A else 1000
		O=f"{A.base_url}chart/OHLCChart/gap-chart";P={'timeFrame':L,'symbols':[A.symbol],'to':K,'countBack':E};M=send_request(url=O,headers=A.headers,method=_D,payload=P,show_log=show_log,proxy_list=A.proxy_config.proxy_list,proxy_mode=A.proxy_config.proxy_mode,request_mode=A.proxy_config.request_mode,hf_proxy_url=A.proxy_config.hf_proxy_url)
		if not M:raise ValueError('Không tìm thấy dữ liệu. Vui lòng kiểm tra lại mã chứng khoán hoặc thời gian truy xuất.')
		B=ohlc_to_df(data=M[0],column_map=_OHLC_MAP,dtype_map=_OHLC_DTYPE,asset_type=A.asset_type,symbol=A.symbol,source=A.data_source,interval=F.interval,floating=floating,resample_map=_RESAMPLE_MAP);B=B[B[_E]>=H].reset_index(drop=_C)
		if C is not _A:B=B.tail(C)
		if to_df:return B
		else:return B.to_json(orient=_F)
	@agg_execution(_G)
	def intraday(self,page_size:Optional[int]=100,last_time:Optional[str]=_A,to_df:Optional[bool]=_C,show_log:bool=_B)->Union[pd.DataFrame,str]:
		C=page_size;A=self;B=trading_hours(_A)
		if B[_H]is _B and B[_I]==_J:raise ValueError(f"{B[_E]}: Dữ liệu khớp lệnh không thể truy cập trong thời gian chuẩn bị phiên mới. Vui lòng quay lại sau.")
		if A.symbol is _A:raise ValueError(_K)
		if C>30000:logger.warning('Bạn đang yêu cầu truy xuất quá nhiều dữ liệu, điều này có thể gây lỗi quá tải.')
		E=f"{A.base_url}{_INTRADAY_URL}/LEData/getAll";F={_L:A.symbol,'limit':C,'truncTime':last_time};G=send_request(url=E,headers=A.headers,method=_D,payload=F,show_log=show_log,proxy_list=A.proxy_config.proxy_list,proxy_mode=A.proxy_config.proxy_mode,request_mode=A.proxy_config.request_mode,hf_proxy_url=A.proxy_config.hf_proxy_url);D=intraday_to_df(data=G,column_map=_INTRADAY_MAP,dtype_map=_INTRADAY_DTYPE,symbol=A.symbol,asset_type=A.asset_type,source=A.data_source)
		if to_df:return D
		else:return D.to_json(orient=_F)
	@agg_execution(_G)
	def price_depth(self,to_df:Optional[bool]=_C,show_log:Optional[bool]=_B)->Union[pd.DataFrame,str]:
		A=self;C=trading_hours(_A)
		if C[_H]is _B and C[_I]==_J:raise ValueError(f"{C[_E]}: Dữ liệu khớp lệnh không thể truy cập trong thời gian chuẩn bị phiên mới. Vui lòng quay lại sau.")
		if A.symbol is _A:raise ValueError(_K)
		D=f"{A.base_url}{_INTRADAY_URL}/AccumulatedPriceStepVol/getSymbolData";E={_L:A.symbol};F=send_request(url=D,headers=A.headers,method=_D,payload=E,show_log=show_log,proxy_list=A.proxy_config.proxy_list,proxy_mode=A.proxy_config.proxy_mode,request_mode=A.proxy_config.request_mode,hf_proxy_url=A.proxy_config.hf_proxy_url);B=pd.DataFrame(F);B=B[_PRICE_DEPTH_MAP.keys()];B.rename(columns=_PRICE_DEPTH_MAP,inplace=_C);B.source=A.data_source
		if to_df:return B
		else:return B.to_json(orient=_F)