_S='net_asset_percent'
_R='asset_holding'
_Q='nav_report'
_P='industry_holding'
_O='top_holding'
_N='TRADING_FUND'
_M='NEW_FUND'
_L='searchField'
_K='pageSize'
_J='short_name'
_I='GET'
_H='POST'
_G='assetPercent'
_F='SSISCA'
_E='industry'
_D=True
_C='data'
_B='FMK.ext'
_A=False
import json,pandas as pd
from pandas import json_normalize
from typing import Union,List
from datetime import datetime
from vnstock.explorer.fmarket.const import _BASE_URL,_FUND_TYPE_MAPPING,_FUND_LIST_COLUMNS,_FUND_LIST_MAPPING
from vnstock.core.utils.logger import get_logger
from vnstock_data.core.utils.user_agent import get_headers
from vnstock_data.core.utils.client import send_request
from vnai import optimize_execution
logger=get_logger(__name__)
def convert_unix_to_datetime(df_to_convert:pd.DataFrame,columns:List[str])->pd.DataFrame:
	A=df_to_convert.copy()
	for B in columns:A[B]=pd.to_datetime(A[B],unit='ms',utc=_D,errors='coerce').dt.strftime('%Y-%m-%d');A[B]=A[B].where(A[B].ge('1970-01-01'))
	return A
class Fund:
	def __init__(A,random_agent:bool=_A)->None:B=random_agent;A.random_agent=B;A.data_source='fmarket';A.headers=get_headers(data_source=A.data_source,random_agent=B);A.base_url=_BASE_URL;A.fund_list=A.listing()[_J].to_list();A.details=A.FundDetails(A)
	@optimize_execution(_B)
	def listing(self,fund_type:str='')->pd.DataFrame:
		B=fund_type;B=B.upper();D=_FUND_TYPE_MAPPING.get(B,[])
		if B not in{'','BALANCED','BOND','STOCK'}:logger.warning(f"Unsupported fund type: '{B}'. Please choose from: '' to get all funds or specify one of 'BALANCED', 'BOND', or 'STOCK'.")
		E={'types':[_M,_N],'issuerIds':[],'sortOrder':'DESC','sortField':'navTo6Months','page':1,_K:100,'isIpo':_A,'fundAssetTypes':D,'bondRemainPeriods':[],_L:'','isBuyByReward':_A,'thirdAppIds':[]};F=f"{_BASE_URL}/filter"
		try:G=client.send_request(url=F,method=_H,headers=self.headers,payload=E,show_log=_A);C=G;logger.info(f"Total number of funds currently listed on Fmarket: {C[_C]["total"]}");A=json_normalize(C,record_path=[_C,'rows']);A=A[_FUND_LIST_COLUMNS];A=convert_unix_to_datetime(df_to_convert=A,columns=['firstIssueAt','productNavChange.updateAt']);A=A.sort_values(by='productNavChange.navTo36Months',ascending=_A);A.rename(columns=_FUND_LIST_MAPPING,inplace=_D);A=A.reset_index(drop=_D);return A
		except Exception as H:logger.error(f"Error in API response: {str(H)}");raise
	class FundDetails:
		def __init__(A,parent):A.parent=parent
		@optimize_execution(_B)
		def top_holding(self,symbol=_F)->pd.DataFrame:return self._get_fund_details(symbol,_O)
		@optimize_execution(_B)
		def industry_holding(self,symbol=_F)->pd.DataFrame:return self._get_fund_details(symbol,_P)
		@optimize_execution(_B)
		def nav_report(self,symbol=_F)->pd.DataFrame:return self._get_fund_details(symbol,_Q)
		@optimize_execution(_B)
		def asset_holding(self,symbol=_F)->pd.DataFrame:return self._get_fund_details(symbol,_R)
		def _get_fund_details(B,symbol,section)->pd.DataFrame:
			C=section;A=symbol;A=A.upper()
			if A not in B.parent.fund_list:logger.error(f"Error: {A} is not a valid input. Call the listing() method for the list of valid Fund short_name.");raise ValueError(f"Invalid symbol: {A}")
			try:G=int(B.parent.filter(A)['id'][0]);logger.info(f"Retrieving data for {A}")
			except Exception as D:logger.error(f"An unexpected error occurred: {str(D)}");raise
			E={_O:B.parent.top_holding,_P:B.parent.industry_holding,_Q:B.parent.nav_report,_R:B.parent.asset_holding}
			if C in E:
				try:F=E[C](fundId=G)
				except KeyError as D:logger.error(f"Error: Missing expected columns in the response data - {str(D)}");raise ValueError(f"Missing expected columns in the response data - {str(D)}")
				F[_J]=A;return F
			else:logger.error(f"Error: {C} is not a valid input. 4 current options are: top_holding, industry_holding, nav_report, asset_holding");raise ValueError(f"Invalid section: {C}")
	@optimize_execution(_B)
	def filter(self,symbol:str='')->pd.DataFrame:
		A=symbol;A=A.upper();C={_L:A,'types':[_M,_N],_K:100};D=f"{_BASE_URL}/filter"
		try:
			E=client.send_request(url=D,method=_H,headers=self.headers,payload=C,show_log=_A);F=E;B=json_normalize(F,record_path=[_C,'rows'])
			if not B.empty:G=['id','shortName'];B=B[G];return B
			else:raise ValueError(f"No fund found with this symbol {A}. See funds_listing() for the list of valid Fund short names.")
		except Exception as H:logger.error(f"Error in API response: {str(H)}");raise
	@optimize_execution(_B)
	def top_holding(self,fundId:int=23)->pd.DataFrame:
		J='type';I='netAssetPercent';H='stockCode';G='fundId';E=fundId;D='updateAt';K=f"{_BASE_URL}/{E}"
		try:
			L=client.send_request(url=K,method=_I,headers=self.headers,show_log=_A);F=L;A=pd.DataFrame();B=json_normalize(F,record_path=[_C,'productTopHoldingList'])
			if not B.empty:B=convert_unix_to_datetime(df_to_convert=B,columns=[D]);A=pd.concat([A,B])
			C=json_normalize(F,record_path=[_C,'productTopHoldingBondList'])
			if not C.empty:C=convert_unix_to_datetime(df_to_convert=C,columns=[D]);A=pd.concat([A,C])
			if not A.empty:A[G]=int(E);M=[H,_E,I,J,D,G];N=[B for B in M if B in A.columns];A=A[N];O={H:'stock_code',_E:_E,I:_S,J:'type_asset',D:'update_at'};P={B:C for(B,C)in O.items()if B in A.columns};A.rename(columns=P,inplace=_D);return A
			else:logger.warning(f"No data available for fundId {E}.");return pd.DataFrame()
		except Exception as Q:logger.error(f"Error in API response: {str(Q)}");raise
	@optimize_execution(_B)
	def industry_holding(self,fundId:int=23)->pd.DataFrame:
		B=f"{_BASE_URL}/{fundId}"
		try:C=client.send_request(url=B,method=_I,headers=self.headers,show_log=_A);D=C;A=json_normalize(D,record_path=[_C,'productIndustriesHoldingList']);E=[_E,_G];F=[B for B in E if B in A.columns];A=A[F];G={_E:_E,_G:_S};H={B:C for(B,C)in G.items()if B in A.columns};A.rename(columns=H,inplace=_D);return A
		except Exception as I:logger.error(f"Error in API response: {str(I)}");raise
	@optimize_execution(_B)
	def nav_report(self,fundId:int=23)->pd.DataFrame:
		D='nav';C='navDate';B=fundId;E=datetime.now().strftime('%Y%m%d');F=f"{_BASE_URL[:-1]}/get-nav-history";G={'isAllData':1,'productId':B,'fromDate':None,'toDate':E}
		try:
			H=client.send_request(url=F,method=_H,headers=self.headers,payload=G,show_log=_A);I=H;A=json_normalize(I,record_path=[_C])
			if not A.empty:J=[C,D];K=[B for B in J if B in A.columns];A=A[K];L={C:'date',D:'nav_per_unit'};M={B:C for(B,C)in L.items()if B in A.columns};A.rename(columns=M,inplace=_D);return A
			else:raise ValueError(f"No data with this fund_id {B}")
		except Exception as N:logger.error(f"Error in API response: {str(N)}");raise
	@optimize_execution(_B)
	def asset_holding(self,fundId:int=23)->pd.DataFrame:
		B='assetType.name';C=f"{_BASE_URL}/{fundId}"
		try:D=client.send_request(url=C,method=_I,headers=self.headers,show_log=_A);E=D;A=json_normalize(E,record_path=[_C,'productAssetHoldingList']);F=[_G,B];G=[B for B in F if B in A.columns];A=A[G];H={_G:'asset_percent',B:'asset_type'};I={B:C for(B,C)in H.items()if B in A.columns};A.rename(columns=I,inplace=_D);return A
		except Exception as J:logger.error(f"Error in API response: {str(J)}");raise