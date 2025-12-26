_M='exchange_rate'
_L='last_updated'
_K='quarter'
_J=True
_I='day'
_H='2025-04'
_G='2015-01'
_F='report_time'
_E='MBK'
_D=False
_C='year'
_B='group_name'
_A='month'
import pandas as pd
from datetime import datetime,date
from vnai import agg_execution
from vnstock.core.utils.parser import camel_to_snake
from vnstock.core.utils.transform import reorder_cols
from vnstock_data.core.utils.client import send_request
from vnstock_data.core.utils.user_agent import get_headers
from vnstock_data.explorer.mbk.const import _BASE_URL,MACRO_DATA,REPORT_PERIOD,TYPE_ID
def process_report_dates(df,last_updated_col,keep_label=_J):
	O='9 tháng';N='6 tháng';M='%Y-%m-%d';I='Năm';B=df;J=[];K=[]
	for(F,G)in zip(B.index,B[last_updated_col]):
		if isinstance(G,date):L=G
		else:L=datetime.strptime(str(G),M).date()
		if'/'in str(F):A,C=str(F).split('/');C=int(C)
		else:
			A=str(F)
			try:C=int(A);A=I
			except ValueError:raise ValueError(f"Unrecognized report type: {A}")
		if'Quý'in A:
			H=int(A.split(' ')[1]);P=H*3;D=date(C,P,1)
			if H==4 and L.year>C:D=date(C,12,31)
			E=f"Quý {H}"
		elif N in A:D=date(C,6,30);E=N
		elif O in A:D=date(C,9,30);E=O
		elif I in A or A.isdigit():D=date(C,12,31);E=I
		else:raise ValueError(f"Unrecognized report type: {A}")
		J.append(D.strftime(M));K.append(E)
	B=B.copy()
	if keep_label:B['label']=B.index
	B.index=J;B['report_type']=K;return B
class Macro:
	def __init__(A,random_agent:bool=_D,show_log:bool=_D):A.data_source=_E;A.headers=get_headers(data_source=A.data_source,random_agent=random_agent);A.headers['content-type']='application/x-www-form-urlencoded; charset=UTF-8';A.show_log=show_log
	def _fetch_macro_data(O,indicator:str,start:str,end:str,period:str=_K)->pd.DataFrame:
		R='Tháng ';N=indicator;M='-';H='0';G=period;F='';C=end;B=start;S=f"{_BASE_URL}{MACRO_DATA}";P=REPORT_PERIOD.get(G)
		if P is None:raise ValueError(f"Unsupported report period type: {G}")
		if N==_M and G==_I:
			I=B.split(M);J=C.split(M)
			if len(I)!=3 or len(J)!=3:raise ValueError("For exchange_rate with period='day', the date format must be 'YYYY-mm-dd'")
			K=I[0];L=J[0];D=B;E=C
		elif G==_C:
			if isinstance(B,int)and 1900<=B<=2100:K=str(B);D=H
			elif isinstance(B,str)and B.isdigit()and len(B)==4 and 1900<=int(B)<=2100:K=B;D=H
			else:raise ValueError("For period='year', start must be integer or string 'YYYY' (e.g. 2020)")
			if isinstance(C,int)and 1900<=C<=2100:L=str(C);E=H
			elif isinstance(C,str)and C.isdigit()and len(C)==4 and 1900<=int(C)<=2100:L=C;E=H
			else:raise ValueError("For period='year', end must be integer or string 'YYYY' (e.g. 2023)")
		else:
			I=B.split(M)
			if len(I)!=2:raise ValueError("Start date format must be 'YYYY-mm'")
			K,D=I;J=C.split(M)
			if len(J)!=2:raise ValueError("End date format must be 'YYYY-mm'")
			L,E=J
			if G==_K:T=(int(D)-1)//3;U=(int(E)-1)//3;D=str(T);E=str(U)
			elif G==_A:D=str(int(D));E=str(int(E))
			else:D=H;E=H
		Q=TYPE_ID.get(N)
		if Q is None:raise ValueError(f"{N} report type code is not defined in TYPE_ID mapping")
		V=f"type={P}&fromYear={K}&toYear={L}&from={D}&to={E}&normTypeID={Q}";W=send_request(url=S,headers=O.headers,method='POST',payload=V,show_log=O.show_log);A=pd.DataFrame(W);A.columns=[camel_to_snake(A)for A in A.columns];A.columns=A.columns.str.replace('tern_',F,regex=_D).str.replace('norm_',F,regex=_D).str.replace('term_',F,regex=_D).str.replace('from_',F,regex=_D).str.replace('_code',F,regex=_D);X=pd.to_numeric(A[_I].str.replace('/Date(',F,regex=_D).str.replace(')/',F,regex=_D));A[_I]=pd.to_datetime(X,unit='ms').dt.date;Y=['report_data_id','id',_C,'group_id','css_style','type_id'];A=A.drop(columns=Y,errors='ignore');A=reorder_cols(A,cols=['unit','source'],position='last');A=reorder_cols(A,cols=[_F],position='first')
		if A[_F].str.contains(R).any():A[_F]=A[_F].str.replace(R,F,regex=_D);A[_F]=A[_F].apply(lambda x:pd.Period(x,freq='M').end_time.date()if isinstance(x,str)else x)
		A.set_index(_F,inplace=_J);A=A.sort_index(ascending=_J);A.rename(columns={_I:_L},inplace=_J);return A
	@agg_execution(_E)
	def gdp(self,start:str=_G,end:str=_H,period:str=_K,keep_label:bool=_D)->pd.DataFrame:
		B=period
		if B not in[_K,_C]:raise ValueError(f"Unsupported report period type: {B}")
		A=self._fetch_macro_data('gdp',start,end,B);A=process_report_dates(A,last_updated_col=_L,keep_label=keep_label);A.index.name=_F;A=A.sort_values([_L,_B,'name']);return A
	@agg_execution(_E)
	def cpi(self,start:str=_G,end:str=_H,period:str=_A)->pd.DataFrame:
		B=period
		if B not in[_A,_C]:raise ValueError(f"Unsupported report period type: {B}")
		A=self._fetch_macro_data('cpi',start,end,B)
		if _B in A.columns:A=A.drop(columns=[_B])
		return A
	@agg_execution(_E)
	def industry_prod(self,start:str=_G,end:str=_H,period:str=_A)->pd.DataFrame:
		A=period
		if A not in[_A,_C]:raise ValueError(f"Unsupported report period type: {A}")
		return self._fetch_macro_data('industrial_production',start,end,A)
	@agg_execution(_E)
	def import_export(self,start:str=_G,end:str=_H,period:str=_A)->pd.DataFrame:
		B=period
		if B not in[_A,_C]:raise ValueError(f"Unsupported report period type: {B}")
		A=self._fetch_macro_data('export_import',start,end,B)
		if _B in A.columns:A=A.drop(columns=[_B])
		return A
	@agg_execution(_E)
	def retail(self,start:str=_G,end:str=_H,period:str=_A)->pd.DataFrame:
		B=period
		if B not in[_A,_C]:raise ValueError(f"Unsupported report period type: {B}")
		A=self._fetch_macro_data('retail',start,end,B)
		if _B in A.columns:A=A.drop(columns=[_B])
		return A
	@agg_execution(_E)
	def fdi(self,start:str=_G,end:str=_H,period:str=_A)->pd.DataFrame:
		A=period
		if A not in[_A,_C]:raise ValueError(f"Unsupported report period type: {A}")
		return self._fetch_macro_data('fdi',start,end,A)
	@agg_execution(_E)
	def money_supply(self,start:str=_G,end:str=_H,period:str=_A)->pd.DataFrame:
		B=period
		if B not in[_A,_C]:raise ValueError(f"Unsupported report period type: {B}")
		A=self._fetch_macro_data('money_supply',start,end,B)
		if _B in A.columns:A=A.drop(columns=[_B])
		return A
	@agg_execution(_E)
	def exchange_rate(self,start:str='2025-01-02',end:str='2025-04-10',period:str=_I)->pd.DataFrame:
		B=period
		if B not in[_I,_C]:raise ValueError(f"Unsupported report period type: {B}")
		A=self._fetch_macro_data(_M,start,end,B)
		if _B in A.columns:A=A.drop(columns=[_B])
		A.index=pd.to_datetime(A.index,dayfirst=_J);return A
	@agg_execution(_E)
	def population_labor(self,start:str=_G,end:str=_H,period:str=_C)->pd.DataFrame:
		B=period
		if B not in[_A,_C]:raise ValueError(f"Unsupported report period type: {B}")
		A=self._fetch_macro_data('population_labor',start,end,B)
		if _B in A.columns:A=A.drop(columns=[_B])
		return A