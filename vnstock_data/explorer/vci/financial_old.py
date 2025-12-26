_S='(đồng)'
_R='(Tỷ đồng)'
_Q='en_name'
_P='quarter'
_O='Chỉ tiêu kết quả kinh doanh'
_N='Chỉ tiêu lưu chuyển tiền tệ'
_M='Chỉ tiêu cân đối kế toán'
_L='lengthReport'
_K='ticker'
_J='final'
_I='name'
_H=', '
_G='com_type_code'
_F='VCI.ext'
_E=True
_D='field_name'
_C='en'
_B=None
_A=False
import json,pandas as pd
from typing import Optional,List,Dict,Tuple,Union
from vnstock.explorer.vci.const import _GRAPHQL_URL,_FINANCIAL_REPORT_PERIOD_MAP,_UNIT_MAP,_ICB4_COMTYPE_CODE_MAP,SUPPORTED_LANGUAGES
from vnstock.explorer.vci import Company
from vnstock_data.core.utils.client import send_request,ProxyConfig
from vnstock.core.utils.parser import get_asset_type,camel_to_snake
from vnstock.core.utils.logger import get_logger
from vnstock.core.utils.user_agent import get_headers
from vnstock.core.utils.transform import replace_in_column_names,flatten_hierarchical_index
from vnai import agg_execution
logger=get_logger(__name__)
class Finance:
	def __init__(A,symbol:str,period:Optional[str]=_P,get_all:Optional[bool]=_E,proxy_config:Optional[ProxyConfig]=_B,show_log:Optional[bool]=_A):
		D=show_log;C=proxy_config;B=period;A.symbol=symbol.upper();A.asset_type=get_asset_type(A.symbol);A.headers=get_headers(data_source='VCI');A.show_log=D;A.proxy_config=C if C is not _B else ProxyConfig()
		if not D:logger.setLevel('CRITICAL')
		if B not in['year',_P]:raise ValueError("Kỳ báo cáo tài chính không hợp lệ. Chỉ chấp nhận 'year' hoặc 'quarter'.")
		if A.asset_type not in['stock']:raise ValueError('Mã chứng khoán không hợp lệ. Chỉ cổ phiếu mới có thông tin.')
		A.period=_FINANCIAL_REPORT_PERIOD_MAP.get(B);A.get_all=get_all;A.com_type_code=A._get_company_type()
	def _get_company_type(A)->str:B=Company(symbol=A.symbol)._fetch_data()['CompanyListingInfo'];C=_ICB4_COMTYPE_CODE_MAP[B['icbName4']];return C
	@staticmethod
	def duplicated_columns_handling(df_or_mapping,target_col_name=_B):
		C=target_col_name;A=df_or_mapping
		if C is not _B:G=A[A[C].duplicated()].copy();J=A[~A[C].duplicated()].copy();G[C]=A[_I]+' - '+A[_D];return pd.concat([J,G])
		else:
			B=A.copy();K=B.columns.duplicated(keep=_A);H=B.columns[K].unique()
			if len(H)>0:
				D=B.columns.tolist()
				for E in H:
					L=[A for(A,B)in enumerate(D)if B==E]
					for M in L[1:]:
						F=f"_{E}";I=1
						while F in D:F=f"{"_"*(I+1)}{E}";I+=1
						D[M]=F
				B.columns=D
			return B
	def _get_ratio_dict(B,show_log:Optional[bool]=_A,get_all:Optional[bool]=_A)->pd.DataFrame:
		E=get_all;D=show_log;C='unit';F='{"query":"query Query {\\n  ListFinancialRatio {\\n    id\\n    type\\n    name\\n    unit\\n    isDefault\\n    fieldName\\n    en_Type\\n    en_Name\\n    tagName\\n    comTypeCode\\n    order\\n    __typename\\n  }\\n}\\n","variables":{}}';G=json.loads(F)
		if D:logger.debug(f"Requesting financial ratio data from {_GRAPHQL_URL}. payload: {F}")
		H=send_request(url=_GRAPHQL_URL,headers=B.headers,method='POST',payload=G,show_log=D,proxy_list=B.proxy_config.proxy_list,proxy_mode=B.proxy_config.proxy_mode,request_mode=B.proxy_config.request_mode,hf_proxy_url=B.proxy_config.hf_proxy_url);I=H['data']['ListFinancialRatio'];A=pd.DataFrame(I);A.columns=[camel_to_snake(A).replace('__','_')for A in A.columns];J=E if E is not _B else B.get_all;K=[_D,_I,_Q,'type','order',C,_G];A[C]=A[C].map(_UNIT_MAP)
		if J is _A:A=A[K]
		A.columns=[A.replace('__','_')for A in A.columns];return A
	def _get_report(A,period:Optional[str]=_B,lang:Optional[str]=_C,show_log:Optional[bool]=_A,mode:Optional[str]=_J)->Union[Tuple[Dict[str,pd.DataFrame],pd.DataFrame],pd.DataFrame]:
		I='variables';E=mode;D=show_log;C=lang;B=period
		if C not in SUPPORTED_LANGUAGES:J=_H.join(SUPPORTED_LANGUAGES);raise ValueError(f"Ngôn ngữ không hợp lệ: '{C}'. Các ngôn ngữ được hỗ trợ: {J}.")
		K=_FINANCIAL_REPORT_PERIOD_MAP.get(B,B)if B else A.period;G='{"query":"fragment Ratios on CompanyFinancialRatio {\\n  ticker\\n  yearReport\\n  lengthReport\\n  updateDate\\n  revenue\\n  revenueGrowth\\n  netProfit\\n  netProfitGrowth\\n  ebitMargin\\n  roe\\n  roic\\n  roa\\n  pe\\n  pb\\n  eps\\n  currentRatio\\n  cashRatio\\n  quickRatio\\n  interestCoverage\\n  ae\\n  netProfitMargin\\n  grossMargin\\n  ev\\n  issueShare\\n  ps\\n  pcf\\n  bvps\\n  evPerEbitda\\n  BSA1\\n  BSA2\\n  BSA5\\n  BSA8\\n  BSA10\\n  BSA159\\n  BSA16\\n  BSA22\\n  BSA23\\n  BSA24\\n  BSA162\\n  BSA27\\n  BSA29\\n  BSA43\\n  BSA46\\n  BSA50\\n  BSA209\\n  BSA53\\n  BSA54\\n  BSA55\\n  BSA56\\n  BSA58\\n  BSA67\\n  BSA71\\n  BSA173\\n  BSA78\\n  BSA79\\n  BSA80\\n  BSA175\\n  BSA86\\n  BSA90\\n  BSA96\\n  CFA21\\n  CFA22\\n  at\\n  fat\\n  acp\\n  dso\\n  dpo\\n  ccc\\n  de\\n  le\\n  ebitda\\n  ebit\\n  dividend\\n  RTQ10\\n  charterCapitalRatio\\n  RTQ4\\n  epsTTM\\n  charterCapital\\n  fae\\n  RTQ17\\n  CFA26\\n  CFA6\\n  CFA9\\n  BSA85\\n  CFA36\\n  BSB98\\n  BSB101\\n  BSA89\\n  CFA34\\n  CFA14\\n  ISB34\\n  ISB27\\n  ISA23\\n  ISS152\\n  ISA102\\n  CFA27\\n  CFA12\\n  CFA28\\n  BSA18\\n  BSB102\\n  BSB110\\n  BSB108\\n  CFA23\\n  ISB41\\n  BSB103\\n  BSA40\\n  BSB99\\n  CFA16\\n  CFA18\\n  CFA3\\n  ISB30\\n  BSA33\\n  ISB29\\n  CFS200\\n  ISA2\\n  CFA24\\n  BSB105\\n  CFA37\\n  ISS141\\n  BSA95\\n  CFA10\\n  ISA4\\n  BSA82\\n  CFA25\\n  BSB111\\n  ISI64\\n  BSB117\\n  ISA20\\n  CFA19\\n  ISA6\\n  ISA3\\n  BSB100\\n  ISB31\\n  ISB38\\n  ISB26\\n  BSA210\\n  CFA20\\n  CFA35\\n  ISA17\\n  ISS148\\n  BSB115\\n  ISA9\\n  CFA4\\n  ISA7\\n  CFA5\\n  ISA22\\n  CFA8\\n  CFA33\\n  CFA29\\n  BSA30\\n  BSA84\\n  BSA44\\n  BSB107\\n  ISB37\\n  ISA8\\n  BSB109\\n  ISA19\\n  ISB36\\n  ISA13\\n  ISA1\\n  BSB121\\n  ISA14\\n  BSB112\\n  ISA21\\n  ISA10\\n  CFA11\\n  ISA12\\n  BSA15\\n  BSB104\\n  BSA92\\n  BSB106\\n  BSA94\\n  ISA18\\n  CFA17\\n  ISI87\\n  BSB114\\n  ISA15\\n  BSB116\\n  ISB28\\n  BSB97\\n  CFA15\\n  ISA11\\n  ISB33\\n  BSA47\\n  ISB40\\n  ISB39\\n  CFA7\\n  CFA13\\n  ISS146\\n  ISB25\\n  BSA45\\n  BSB118\\n  CFA1\\n  CFS191\\n  ISB35\\n  CFB65\\n  CFA31\\n  BSB113\\n  ISB32\\n  ISA16\\n  CFS210\\n  BSA48\\n  BSA36\\n  ISI97\\n  CFA30\\n  CFA2\\n  CFB80\\n  CFA38\\n  CFA32\\n  ISA5\\n  BSA49\\n  CFB64\\n  __typename\\n}\\n\\nquery Query($ticker: String!, $period: String!) {\\n  CompanyFinancialRatio(ticker: $ticker, period: $period) {\\n    ratio {\\n      ...Ratios\\n      __typename\\n    }\\n    period\\n    __typename\\n  }\\n}\\n","variables":{"ticker":"VCI","period":"Q"}}';F=json.loads(G);F[I][_K]=A.symbol;F[I]['period']=K
		if D:logger.debug(f"Requesting financial report data from {_GRAPHQL_URL}. payload: {G}")
		L=send_request(url=_GRAPHQL_URL,headers=A.headers,method='POST',payload=F,show_log=D,proxy_list=A.proxy_config.proxy_list,proxy_mode=A.proxy_config.proxy_mode,request_mode=A.proxy_config.request_mode,hf_proxy_url=A.proxy_config.hf_proxy_url)
		try:
			M=L['data']['CompanyFinancialRatio']['ratio'];H=pd.DataFrame(M)
			if E==_J:N,O=A._ratio_mapping(H,lang=C,show_log=D);return N,O
			elif E=='raw':return H
			else:raise ValueError(f"Invalid mode: {E}. Must be 'final' or 'raw'.")
		except Exception as P:logger.error(f"Error processing financial report data: {P}");raise
	@agg_execution(_F)
	def _ratio_mapping(self,ratio_df:pd.DataFrame,lang:Optional[str]=_C,mode:str=_J,show_log:Optional[bool]=_A):
		U='Năm';T='yearReport';N='CT';L=lang;C=self;A=ratio_df
		if L=='vi':A=A.rename(columns={_K:'CP',T:U,_L:'Kỳ'});M=['CP',U,'Kỳ'];D=A[M];J=_I
		elif L==_C:M=[_K,T,_L];D=A[M];J=_Q
		B=C._get_ratio_dict(get_all=_A)
		if C.com_type_code!=N:O=B[B[_G]==C.com_type_code]
		else:O=pd.DataFrame()
		V=B[B[_G]==N];E=pd.concat([O,V]).drop_duplicates(subset=_D,keep='first');E=E[E[_G].isin([N,C.com_type_code])].copy();W=A.columns;P=[A for A in W if A not in B[_D].values and A not in D.columns]
		if show_log:logger.debug(f"Orphan columns will be dropped: {P}")
		Q=E.set_index(_D)[J].to_dict();X=E.set_index(J)['order'].to_dict();A=A.drop(columns=P);A=A[sorted(A.columns,key=lambda x:X.get(x,0))];Y=E.groupby('type')[_D].apply(list).to_dict();C.raw_ratio_df=A.copy();C.columns_mapping=Q;K={}
		for(Z,a)in Y.items():K[Z]=A[a]
		R=[_M,_N,_O];F={A:K[A]for A in R};G={A:K[A]for A in K if A not in R};S=dict(zip(B[_D],B[J]))
		for H in F:F[H].columns=[S.get(A,A)for A in F[H].columns]
		for H in G:G[H].columns=[S.get(A,A)for A in G[H].columns]
		I=pd.concat(G.values(),axis=1,keys=G.keys());F={A:pd.concat([D,B],axis=1)for(A,B)in F.items()};D.columns=pd.MultiIndex.from_tuples([('Meta',A)for A in D.columns]);I=pd.concat([D,I],axis=1)
		def b(col,translation_dict):B=col[:-1];A=col[-1];C=translation_dict.get(A,A);return B+(C,)
		if L==_C:I.columns=pd.MultiIndex.from_tuples([b(A,Q)for A in I.columns])
		return F,I
	def _process_report(C,report_key:str,period:Optional[str]=_B,lang:Optional[str]=_C,dropna:Optional[bool]=_A,show_log:Optional[bool]=_A)->pd.DataFrame:
		I='ignore';E=lang;D=period;B=report_key;G=[_O,_M,_N]
		if B not in G:raise ValueError(f"Báo cáo không hợp lệ. Chỉ chấp nhận {_H.join(G)}.")
		H=_FINANCIAL_REPORT_PERIOD_MAP.get(D,D)if D else C.period
		try:
			F=C._get_report(period=H,lang=E,show_log=show_log)[0]
			if B not in F:J=_H.join(F.keys());raise KeyError(f"Báo cáo '{B}' không có sẵn. Các báo cáo có sẵn: {J}")
			A=F[B]
			if dropna:A=A.fillna(0);A=A.loc[:,(A!=0).any(axis=0)]
			if H=='Y':
				if E==_C:A=A.drop(columns=_L,errors=I)
				elif E=='vi':A=A.drop(columns='Kỳ',errors=I)
			A=replace_in_column_names(A,_R,_S);A=C.duplicated_columns_handling(A);return A
		except Exception as K:logger.error(f"Error processing report '{B}': {K}");raise
	@agg_execution(_F)
	def balance_sheet(self,period:Optional[str]=_B,lang:Optional[str]=_C,dropna:Optional[bool]=_E,show_log:Optional[bool]=_A)->pd.DataFrame:
		A=lang
		if A not in SUPPORTED_LANGUAGES:raise ValueError(f"Ngôn ngữ không hợp lệ: '{A}'. Các ngôn ngữ được hỗ trợ: {_H.join(SUPPORTED_LANGUAGES)}.")
		try:return self._process_report(_M,period=period,lang=A,dropna=dropna,show_log=show_log)
		except Exception as B:logger.error(f"Error retrieving balance sheet: {B}");raise
	@agg_execution(_F)
	def income_statement(self,period:Optional[str]=_B,lang:Optional[str]=_C,dropna:Optional[bool]=_E,show_log:Optional[bool]=_A)->pd.DataFrame:return self._process_report(_O,period=period,lang=lang,dropna=dropna,show_log=show_log)
	@agg_execution(_F)
	def cash_flow(self,period:Optional[str]=_B,lang:Optional[str]=_C,dropna:Optional[bool]=_E,show_log:Optional[bool]=_A)->pd.DataFrame:return self._process_report(_N,period=period,lang=lang,dropna=dropna,show_log=show_log)
	@agg_execution(_F)
	def ratio(self,period:Optional[str]=_B,lang:Optional[str]=_C,dropna:Optional[bool]=_E,show_log:Optional[bool]=_A,flatten_columns:Optional[bool]=_A,separator:Optional[str]='_',drop_levels:Optional[Union[int,List[int]]]=_B)->pd.DataFrame:
		B=period;C=_FINANCIAL_REPORT_PERIOD_MAP.get(B,B)if B else self.period
		try:
			A=self._get_report(period=C,lang=lang,show_log=show_log)[1]
			if dropna:A=A.fillna(0);A=A.loc[:,(A!=0).any(axis=0)]
			if flatten_columns:D={_R:_S};A=flatten_hierarchical_index(A,separator=separator,text_replacements=D,handle_duplicates=_E,drop_levels=drop_levels)
			return A
		except Exception as E:logger.error(f"Error retrieving financial ratios: {E}");raise