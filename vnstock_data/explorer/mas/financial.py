_K="Kỳ báo cáo tài chính không hợp lệ. Chỉ chấp nhận 'year' hoặc 'quarter'."
_J='period'
_I='ignore'
_H='quarter'
_G='MAS.ext'
_F=False
_E='vi'
_D='year'
_C=True
_B=None
_A='year_period'
import json,pandas as pd
from typing import Optional
from vnstock_data.explorer.mas.const import _FINANCIAL_URL,_FINANCIAL_REPORT_MAP,_FINANCIAL_REPORT_PERIOD_MAP,SUPPORTED_LANGUAGES
from vnstock_data.core.utils.user_agent import get_headers
from vnstock_data.core.utils.parser import get_asset_type
from vnstock.core.utils.transform import replace_in_column_names,flatten_hierarchical_index,reorder_cols
from vnai import agg_execution
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.client import send_request
from vnstock.core.utils.parser import camel_to_snake
logger=get_logger(__name__)
class Finance:
	def __init__(A,symbol:str,period:Optional[str]=_H,get_all:Optional[bool]=_C,show_log:Optional[bool]=_C):
		C=show_log;B=period;A.symbol=symbol.upper();A.asset_type=get_asset_type(A.symbol);A.base_url=_FINANCIAL_URL;A.query_params='query{vsFinancialReportList(StockCode:"TARGET_SYMBOL",Type:"TARGET_TYPE",TermType:"TARGET_PERIOD"){_id,ID,TermCode,YearPeriod,Content{Values{Name,NameEn,Value}}}}';A.headers=get_headers(data_source='MAS');A.show_log=C
		if not C:logger.setLevel('CRITICAL')
		if B not in[_D,_H]:raise ValueError(_K)
		A.raw_period=B;A.period=_FINANCIAL_REPORT_PERIOD_MAP.get(B)
		if A.asset_type not in['stock']:raise ValueError('Mã chứng khoán không hợp lệ. Chỉ cổ phiếu mới có thông tin.')
		A.get_all=get_all
	def _flatten_content(G,content,lang:str)->dict:
		F='Values';C=content
		if isinstance(C,list):
			for B in C:
				if isinstance(B,dict)and F in B:
					D={}
					for A in B[F]:
						if isinstance(A,dict):
							E=A.get('NameEn','').strip()if lang.lower()=='en'else A.get('Name','').strip()
							if E:D[E]=A.get('Value')
					return D
		return{}
	def _parse_nested_data(B,df:pd.DataFrame,lang:str)->pd.DataFrame:A='content';C=df[A].apply(lambda content:B._flatten_content(content,lang));D=pd.DataFrame(C.tolist());E=pd.concat([df.drop(A,axis=1,errors=_I),D],axis=1);return E
	def _clean_columns(F,df:pd.DataFrame,period_type:str)->pd.DataFrame:
		C='term_code';B=period_type;A=df;B=B.lower()
		if B==_D:A[_J]=A[_A]if _A in A.columns else _B
		elif B==_H:
			if _A in A.columns and C in A.columns:A[_J]=A[_A].astype(str)+'-'+A[C].astype(str)
			else:A[_J]=_B
			if _A in A.columns:A=A.drop(_A,axis=1,errors=_I)
		else:raise ValueError("period_type must be either 'year' or 'quarter'")
		D=['_id','id',C];E=[B for B in D if B in A.columns];return A.drop(columns=E,errors=_I)
	def _get_report(B,report_type:str,period:Optional[str],lang:Optional[str],dropna:bool,show_log:bool)->pd.DataFrame:
		D=lang;C=period
		if D not in SUPPORTED_LANGUAGES:raise ValueError(f"Ngôn ngữ không hợp lệ: '{D}'. Hỗ trợ: {', '.join(SUPPORTED_LANGUAGES)}.")
		if C is _B:F=B.raw_period;E=B.period
		elif C not in[_D,_H]:raise ValueError(_K)
		else:F=C;E=_FINANCIAL_REPORT_PERIOD_MAP.get(C)
		G=B.base_url+'financialReport';H=B.query_params.replace('TARGET_SYMBOL',B.symbol).replace('TARGET_TYPE',_FINANCIAL_REPORT_MAP[report_type]).replace('TARGET_PERIOD',E);I={'query':H};J=send_request(url=G,headers=B.headers,method='GET',params=I,payload=_B,show_log=show_log);A=pd.DataFrame(J);A.columns=[camel_to_snake(A)for A in A.columns]
		if A.empty:logger.warning(f"No data found for symbol {B.symbol} in period {E}.");return pd.DataFrame()
		A=B._parse_nested_data(A,D);A=B._clean_columns(A,period_type=F);A=reorder_cols(A,[_J],position='first')
		if dropna:A=A.dropna(axis=1,how='all')
		return A
	@agg_execution(_G)
	def balance_sheet(self,period:Optional[str]=_B,lang:Optional[str]=_E,dropna:Optional[bool]=_C,show_log:Optional[bool]=_F)->pd.DataFrame:return self._get_report('balance_sheet',period,lang,dropna,show_log)
	@agg_execution(_G)
	def income_statement(self,period:Optional[str]=_B,lang:Optional[str]=_E,dropna:Optional[bool]=_C,show_log:Optional[bool]=_F)->pd.DataFrame:return self._get_report('income_statement',period,lang,dropna,show_log)
	@agg_execution(_G)
	def cash_flow(self,period:Optional[str]=_B,lang:Optional[str]=_E,dropna:Optional[bool]=_C,show_log:Optional[bool]=_F)->pd.DataFrame:return self._get_report('cash_flow',period,lang,dropna,show_log)
	@agg_execution(_G)
	def ratio(self,period:Optional[str]=_B,lang:Optional[str]=_E,dropna:Optional[bool]=_C,show_log:Optional[bool]=_F)->pd.DataFrame:return self._get_report('ratio',period,lang,dropna,show_log)
	@agg_execution(_G)
	def annual_plan(self,period:Optional[str]=_D,lang:Optional[str]=_E,dropna:Optional[bool]=_C,show_log:Optional[bool]=_F)->pd.DataFrame:
		B=period
		if B not in[_D]:raise ValueError('Báo cáo chỉ tiêu kế hoạch chỉ chấp nhận giá trị year.')
		A=self._get_report('annual_plan',B,lang,dropna,show_log)
		if _A in A.columns:A=A.drop(_A,axis=1,errors=_I)
		return A