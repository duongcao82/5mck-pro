_I='coerce'
_H=False
_G=None
_F=True
_E='MAS'
_D='VCI'
_C='volume'
_B='match_type'
_A='time'
import pytz,pandas as pd
from bs4 import BeautifulSoup
from typing import Dict,Any,List,Optional,Union
from datetime import datetime,timedelta,time
from vnstock.core.utils.parser import localize_timestamp
from vnstock.core.utils.transform import get_trading_date
def generate_period(df):
	D='report_period';C='lengthReport';A='yearReport';E=[A,C,D]
	for B in E:
		if B not in df.columns:raise ValueError(f"Thiếu cột {B} trong df")
	df.loc[:,'period']=df.apply(lambda x:str(x[A])if x[D]=='year'else f"{x[A]}-Q{x[C]}",axis=1);return df
def remove_pattern_columns(df:pd.DataFrame,patterns:List[str])->pd.DataFrame:A=[A for A in df.columns if any(B in A.lower()for B in patterns)];df.drop(columns=A,inplace=_F);return df
def clean_numeric_string(s:Any)->Any:
	A=','
	if not isinstance(s,str):return s
	s=s.replace('\xa0','').replace(A,'')
	if s.count('.')==0 and s.count(A)==1:s=s.replace(A,'.')
	return s.strip()
def process_match_types(df,asset_type,source):
	G='unknown';E='date';D='Sell';C='Buy';B=source;A=df
	if B==_D:A[_B]=A[_B].replace({'b':C,'s':D})
	elif B==_E:A[_B]=A[_B].replace({'BUY':C,'SELL':D});A[_B]=A[_B].fillna(G)
	elif B=='TCBS':A[_B]=A[_B].replace({'BU':C,'SD':D})
	F=G if B in[_D,_E]else''
	if asset_type=='stock'and(A[_B].eq(F).any()or A[_B].eq('').any()):
		A=A.sort_values(_A);A[E]=A[_A].dt.date
		def H(day_df):
			B=day_df;A=B[B[_B]==F]
			if A.empty:return B
			C=A[(A[_A].dt.hour==9)&A[_A].dt.minute.between(13,17)]
			if not C.empty:E=C[_A].idxmin();B.loc[E,_B]='ATO'
			D=A[(A[_A].dt.hour==14)&A[_A].dt.minute.between(43,47)]
			if not D.empty:G=D[_A].idxmax();B.loc[G,_B]='ATC'
			return B
		A=A.groupby(E,group_keys=_H).apply(H);A.drop(columns=[E],inplace=_F)
	return A
def ohlc_to_df(data:Dict[str,Any],column_map:Dict[str,str],dtype_map:Dict[str,str],asset_type:str,symbol:str,source:str,interval:str='1D',floating:int=2,resample_map:Optional[Dict[str,str]]=_G)->pd.DataFrame:
	P='Asia/Ho_Chi_Minh';O='UTC';L=resample_map;K=asset_type;J=interval;I=source;H=column_map;G=data;F='low';E='high';D='close';C='open'
	if not G:raise ValueError('Input data is empty or not provided.')
	if I=='TCBS':A=pd.DataFrame(G);A.rename(columns=H,inplace=_F)
	else:Q={A:H[A]for A in H.keys()if A in G};A=pd.DataFrame(G)[Q.keys()].rename(columns=H)
	R=[_A,C,E,F,D,_C];M=[B for B in R if B not in A.columns]
	if M:raise ValueError(f"Missing required columns: {M}. Available columns: {A.columns.tolist()}")
	A=A[[_A,C,E,F,D,_C]]
	if _A in A.columns:
		if I==_D:A[_A]=pd.to_datetime(A[_A].astype(int),unit='s').dt.tz_localize(O);A[_A]=A[_A].dt.tz_convert(P)
		elif I==_E:A[_A]=pd.to_datetime(A[_A].astype(float),unit='s').dt.tz_localize(O).dt.tz_convert(P)
		else:A[_A]=pd.to_datetime(A[_A],errors=_I)
	if K not in['index','derivative']:A[[C,E,F,D]]=A[[C,E,F,D]].div(1000)
	A[[C,E,F,D]]=A[[C,E,F,D]].round(floating)
	if L and J not in['1m','1H','1D']:A=A.set_index(_A).resample(L[J]).agg({C:'first',E:'max',F:'min',D:'last',_C:'sum'}).dropna(subset=[C,D]).reset_index()
	for(B,N)in dtype_map.items():
		if B in A.columns:
			if N=='datetime64[ns]'and hasattr(A[B],'dt')and A[B].dt.tz is not _G:
				A[B]=A[B].dt.tz_localize(_G)
				if J=='1D':A[B]=A[B].dt.date
			A[B]=A[B].astype(N)
	A.name=symbol;A.category=K;A.source=I;return A
def intraday_to_df(data:List[Dict[str,Any]],column_map:Dict[str,str],dtype_map:Dict[str,str],symbol:str,asset_type:str,source:str)->pd.DataFrame:
	O='symbol';I=symbol;G=asset_type;F='price';D=column_map;C=source
	if not data:E=pd.DataFrame(columns=list(D.values()));E.attrs[O]=I;E.category=G;E.source=C;return E
	A=pd.DataFrame(data);H=[B for B in D if B in A.columns]
	if not H:raise ValueError(f"Expected columns {list(D)} not found, got {A.columns.tolist()}")
	A=A[H].rename(columns={A:D[A]for A in H})
	for B in(F,_C):
		if B in A.columns:
			A[B]=A[B].map(clean_numeric_string);A[B]=pd.to_numeric(A[B],errors=_I);J=A[B].isna().sum()
			if J:print(f"[Warning] {J} giá trị ở '{B}' không parse được, chuyển thành NaN")
	P={_D:1000,_E:1000};Q=P.get(C,1)
	if F in A.columns:A[F]=A[F]/Q
	if _C in A.columns:
		K=A[_C].fillna(0);L=K%1!=0
		if L.any():print(f"[Info] {int(L.sum())} giá trị volume có decimal, sẽ làm tròn")
		A[_C]=K.round().astype(int)
	if _A in A.columns:
		R=get_trading_date()
		if C==_D:A[_A]=localize_timestamp(A[_A].astype(int),unit='s')
		elif C==_E:A[_A]=localize_timestamp(A[_A].astype(int),unit='ms');A[_A]=A[_A].dt.floor('s')
		else:
			M=str(A[_A].iloc[0])if not A.empty else''
			if':'in M and len(M)<=8:A[_A]=A[_A].apply(lambda x:datetime.combine(R,datetime.strptime(x,'%H:%M:%S').time())if isinstance(x,str)and':'in x else pd.NaT);A[_A]=localize_timestamp(A[_A],return_string=_H)
			else:
				A[_A]=pd.to_datetime(A[_A],format='%Y-%m-%d %H:%M:%S',errors=_I)
				if A[_A].dt.tz is _G:A[_A]=localize_timestamp(A[_A],return_string=_H)
	if _B in A.columns:A=process_match_types(A,G,C)
	if _A in A.columns:A=A.sort_values(_A)
	A=A.reset_index(drop=_F);N={B:C for(B,C)in dtype_map.items()if B in A.columns and B!=_A}
	if N:A=A.astype(N)
	A.attrs[O]=I;A.category=G;A.source=C;return A