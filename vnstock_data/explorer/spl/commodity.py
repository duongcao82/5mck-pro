_C='close'
_B='SPL.ext'
_A=None
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from.spl_fetcher import SPLFetcher
from typing import Dict,Any,Optional,List
from vnai import agg_execution
from vnstock.core.utils.logger import get_logger
logger=get_logger(__name__)
class CommodityPrice:
	def __init__(A,start:Optional[str]=_A,end:Optional[str]=_A,show_log:Optional[bool]=False):
		A.fetcher=SPLFetcher();A.default_start=start;A.default_end=end
		if not show_log:logger.setLevel('CRITICAL')
	def _fetch_commodity(C,ticker:str,start:Optional[str]=_A,end:Optional[str]=_A,interval:str='1d',columns:Optional[List]=_A)->pd.DataFrame:
		K='%Y-%m-%d';J=columns;E=end;D=start;B='time';F={'ticker':ticker,'interval':interval,'type':'commodity'};G=ZoneInfo('Asia/Ho_Chi_Minh');D=D or C.default_start;E=E or C.default_end
		if D:H=datetime.strptime(D,K);H=H.replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=G);F['from']=int(H.timestamp())
		if E:I=datetime.strptime(E,K);I=I.replace(hour=23,minute=59,second=59,microsecond=999999,tzinfo=G);F['to']=int(I.timestamp())
		C.fetcher.validate(F);L=C.fetcher.fetch(endpoint='/historical/prices/ohlcv',params=F);A=C.fetcher.to_dataframe(L['data']);A[B]=pd.to_datetime(A[B])
		if A[B].dt.tz is _A:A[B]=A[B].dt.tz_localize(G)
		else:A[B]=A[B].dt.tz_convert(G)
		A[B]=A[B].dt.tz_localize(_A);A.set_index(B,inplace=True)
		if J is not _A:return A[J]
		return A
	def _gold_vn_buy(A,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return A._fetch_commodity('GOLD:VN:BUY',start,end,columns=[_C])
	def _gold_vn_sell(A,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return A._fetch_commodity('GOLD:VN:SELL',start,end,columns=[_C])
	@agg_execution(_B)
	def gold_vn(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:A=start;C=self._gold_vn_buy(A,end);D=self._gold_vn_sell(A,end);B=pd.concat([C,D],axis=1);B.columns=['buy','sell'];return B
	@agg_execution(_B)
	def gold_global(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('GC=F',start,end)
	@agg_execution(_B)
	def _gas_ron92(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('GAS:RON92:VN',start,end,columns=[_C])
	@agg_execution(_B)
	def _gas_ron95(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('GAS:RON95:VN',start,end,columns=[_C])
	@agg_execution(_B)
	def _oil_do(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('GAS:DO:VN',start,end,columns=[_C])
	@agg_execution(_B)
	def gas_vn(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:C=end;B=start;A=self;E=A._gas_ron92(B,C);F=A._gas_ron95(B,C);G=A._oil_do(B,C);D=pd.concat([F,E,G],axis=1);D.columns=['ron95','ron92','oil_do'];return D
	@agg_execution(_B)
	def oil_crude(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('CL=F',start,end)
	@agg_execution(_B)
	def gas_natural(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('NG=F',start,end)
	@agg_execution(_B)
	def coke(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('ICEEUR:NCF1!',start,end)
	@agg_execution(_B)
	def steel_d10(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('STEEL:D10:VN',start,end,columns=[_C])
	@agg_execution(_B)
	def iron_ore(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('COMEX:TIO1!',start,end)
	@agg_execution(_B)
	def steel_hrc(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('COMEX:HRC1!',start,end)
	@agg_execution(_B)
	def fertilizer_ure(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('CBOT:UME1!',start,end)
	@agg_execution(_B)
	def soybean(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('ZM=F',start,end)
	@agg_execution(_B)
	def corn(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('ZC=F',start,end)
	@agg_execution(_B)
	def sugar(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('SB=F',start,end)
	@agg_execution(_B)
	def pork_north_vn(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('PIG:NORTH:VN',start,end,columns=[_C])
	@agg_execution(_B)
	def pork_china(self,start:Optional[str]=_A,end:Optional[str]=_A)->Dict[str,Any]:return self._fetch_commodity('PIG:CHINA',start,end,columns=[_C])