_R='organ_code'
_Q='time_frame'
_P='content'
_O='foreign'
_N='toDate'
_M='fromDate'
_L='ONE_DAY'
_K='timeFrame'
_J='size'
_I='page'
_H='1D'
_G='ticker'
_F=True
_E='data'
_D='VCI.ext'
_C='trading_date'
_B=False
_A=None
from typing import List,Optional,Union
import pandas as pd,json
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.parser import get_asset_type,camel_to_snake,flatten_data
from vnstock.core.utils.transform import flatten_hierarchical_index
from vnstock_data.core.utils.user_agent import get_headers
from vnstock_data.core.utils.parser import filter_columns_by_language
from vnstock.explorer.vci.const import _TRADING_URL
from vnstock_data.explorer.vci.const import _VCIQ_URL,_REPORT_RESOLUTION
from vnstock_data.core.utils.validation import validate_date
from vnstock.core.utils.transform import flatten_hierarchical_index
from vnai import agg_execution
logger=get_logger(__name__)
from vnstock_data.core.utils.client import ProxyConfig,send_request
class Trading:
	def __init__(A,symbol:Optional[str],random_agent=_B,proxy_config:Optional[ProxyConfig]=_A,show_log:Optional[bool]=_B):
		C=show_log;B=proxy_config;A.symbol=symbol.upper();A.asset_type=get_asset_type(A.symbol);A.base_url=_VCIQ_URL;A.headers=get_headers(data_source='VCI',random_agent=random_agent);A.proxy_config=B if B is not _A else ProxyConfig()
		if not C:logger.setLevel('CRITICAL')
		A.show_log=C
	def _process_dates(C,start:Optional[str],end:Optional[str])->tuple[Optional[str],Optional[str]]:
		B=end;A=start
		if A and B:
			if not validate_date(A)or not validate_date(B):logger.error('Invalid date format. Please use the format YYYY-mm-dd.');return _A,_A
			else:return A.replace('-',''),B.replace('-','')
		return _A,_A
	def _fetch_data(A,endpoint:str,params:dict)->dict:
		B=endpoint;C=f"{A.base_url}/v1/company/{A.symbol}/{B}"
		try:
			D=send_request(C,headers=A.headers,method='GET',params=params,show_log=A.show_log,proxy_list=A.proxy_config.proxy_list,proxy_mode=A.proxy_config.proxy_mode,request_mode=A.proxy_config.request_mode,hf_proxy_url=A.proxy_config.hf_proxy_url)
			if A.show_log:logger.info(f"Successfully fetched data from {B} for {A.symbol}")
			return D
		except Exception as E:logger.error(f"Error fetching data from {B} for {A.symbol}: {E}");raise
	def _to_dataframe(D,data:dict,data_path:Optional[list]=_A)->pd.DataFrame:
		B=data_path
		try:
			if B is _A:B=[_E]
			A=data
			for E in B:
				if isinstance(A,dict)and E in A:A=A[E]
				else:logger.warning(f"Data path {B} not found in response for {D.symbol}");return pd.DataFrame()
			if not A:logger.warning(f"No data found at path {B} for {D.symbol}");return pd.DataFrame()
			if isinstance(A,list):C=pd.DataFrame(A)
			elif isinstance(A,dict):C=pd.DataFrame([A])
			else:logger.error(f"Unexpected data type at path {B}: {type(A)}");return pd.DataFrame()
			if not C.empty:C.columns=[camel_to_snake(A)for A in C.columns]
			return C
		except(KeyError,TypeError,Exception)as F:logger.error(f"Unexpected error processing data for {D.symbol}: {F}");raise
	@agg_execution(_D)
	def price_board(self,symbols_list:List[str],to_df:Optional[bool]=_F,show_log:Optional[bool]=_B,flatten_columns:Optional[bool]=_B,separator:Optional[str]='_',drop_levels:Optional[Union[int,List[int]]]=_A):
		X='time';W='session';V='symbol';U='volume';T='price';N=show_log;M='message_type';L='received_time';K='code';I='bidAsk';E=self;D='match';C='bid_ask';A='listing';O=f"{_TRADING_URL}price/symbols/getList";P=json.dumps({'symbols':symbols_list})
		if N:logger.info(f"Requested URL: {O} with query payload: {P}")
		J=send_request(O,headers=E.headers,method='POST',payload=P,show_log=N,proxy_list=E.proxy_config.proxy_list,proxy_mode=E.proxy_config.proxy_mode,request_mode=E.proxy_config.request_mode,hf_proxy_url=E.proxy_config.hf_proxy_url)
		if not J:raise ConnectionError('Tải dữ liệu không thành công hoặc không có dữ liệu trả về.')
		Q=[]
		for F in J:
			Y={A:F['listingInfo'],I:F[I],D:F['matchPrice']};G=flatten_data(Y)
			try:
				for(H,R)in enumerate(F[I]['bidPrices'],start=1):G[f"bidAsk_bid_{H}_price"]=R[T];G[f"bidAsk_bid_{H}_volume"]=R[U]
				for(H,S)in enumerate(F[I]['askPrices'],start=1):G[f"bidAsk_ask_{H}_price"]=S[T];G[f"bidAsk_ask_{H}_volume"]=S[U]
			except:pass
			Q.append(G)
		B=pd.DataFrame(Q);B.columns=pd.MultiIndex.from_tuples([tuple(camel_to_snake(A)for A in A.split('_',1))for A in B.columns]);Z=[(C,K),(C,V),(C,W),(C,L),(C,M),(C,X),(C,'bid_prices'),(C,'ask_prices'),(A,K),(A,'exercise_price'),(A,'exercise_ratio'),(A,'maturity_date'),(A,'underlying_symbol'),(A,'issuer_name'),(A,L),(A,M),(A,'en_organ_name'),(A,'en_organ_short_name'),(A,'organ_short_name'),(A,_G),(D,K),(D,V),(D,L),(D,M),(D,X),(D,W)];B=B.drop(columns=[A for A in Z if A in B.columns]);B=B.rename(columns={'board':'exchange'},level=1)
		if flatten_columns:B=flatten_hierarchical_index(B,separator=separator,drop_levels=drop_levels,handle_duplicates=_F)
		B.attrs['source']='VCI'
		if to_df:return B
		else:return J
	@agg_execution(_D)
	def summary(self,resolution:str=_H,start:Optional[str]=_A,end:Optional[str]=_A,limit:Optional[int]=100):
		B=self;C,D=B._process_dates(start,end);E={_K:_REPORT_RESOLUTION.get(resolution,_L),_I:0,_J:limit}
		if C and D:E.update({_M:C,_N:D})
		F=B._fetch_data('price-history-summary',E);A=B._to_dataframe(F,[_E])
		if not A.empty and any(_O in A for A in A.columns):A.columns=A.columns.str.replace(_O,'fr',regex=_B)
		return A
	@agg_execution(_D)
	def price_history(self,resolution:str=_H,start:Optional[str]=_A,end:Optional[str]=_A,get_all:Optional[bool]=_B,limit:Optional[int]=100):
		B=self;C,D=B._process_dates(start,end);E={_K:_REPORT_RESOLUTION.get(resolution,_L),_I:0,_J:limit}
		if C and D:E.update({_M:C,_N:D})
		F=B._fetch_data('price-history',E);A=B._to_dataframe(F,[_E,_P]);A.columns=A.columns.str.replace(_O,'fr',regex=_B);G=['id',_G,'stock_type',_Q];H={'open_price':'open','close_price':'close','highest_price':'high','lowest_price':'low','total_match_volume':'matched_volume','total_match_value':'matched_value','total_deal_volume':'deal_volume','total_deal_value':'deal_value'};A=A.drop(columns=[B for B in G if B in A.columns])
		if _C in A.columns:A[_C]=pd.to_datetime(A[_C])
		if get_all is _B:A=A.drop(columns=[A for A in A.columns if'fr_'in A])
		A=A.rename(columns={B:C for(B,C)in H.items()if B in A.columns});return A
	@agg_execution(_D)
	def foreign_trade(self,resolution:str=_H,start:Optional[str]=_A,end:Optional[str]=_A,limit:Optional[int]=100):A=self.price_history(resolution=resolution,start=start,end=end,get_all=_F,limit=limit);A=A[[_C]+[A for A in A.columns if A.startswith('fr_')]];return A
	@agg_execution(_D)
	def prop_trade(self,resolution:str=_H,start:Optional[str]=_A,end:Optional[str]=_A,limit:Optional[int]=100):
		B=self;C,D=B._process_dates(start,end);E={_K:_REPORT_RESOLUTION.get(resolution,_L),_I:0,_J:limit}
		if C and D:E.update({_M:C,_N:D})
		F=B._fetch_data('proprietary-history',E);A=B._to_dataframe(F,[_E,_P]);A.columns=A.columns.str.replace('proprietary','prop',regex=_B);G=['id',_G,_R,_Q];A=A.drop(columns=[B for B in G if B in A.columns])
		if _C in A.columns:
			try:A[_C]=pd.to_datetime(A[_C])
			except Exception as H:logger.warning(f"Failed to convert trading_date to datetime: {H}")
		A=A.dropna(how='all');A=A.reset_index(drop=_F);return A
	@agg_execution(_D)
	def insider_deal(self,limit:Optional[int]=100,lang:str='vi'):
		C={_I:0,_J:limit};D=self._fetch_data('insider-transaction',C);A=self._to_dataframe(D,[_E,_P]);A=filter_columns_by_language(A,lang=lang);E=['id',_G,_R,'display_date1','display_date2','event_code','action_type_code','icb_code_lv1'];A=A.drop(columns=[B for B in E if B in A.columns]);F=['start_date','end_date','public_date']
		for B in F:
			if B in A.columns:
				try:A[B]=pd.to_datetime(A[B],errors='coerce')
				except Exception as G:logger.warning(f"Failed to convert {B} to datetime: {G}")
		A=A.dropna(how='all');A=A.reset_index(drop=_F);return A