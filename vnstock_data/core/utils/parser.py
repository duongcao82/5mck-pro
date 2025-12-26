import re,urllib.parse,pandas as pd,unicodedata
from datetime import datetime,timedelta
def get_asset_type(symbol:str)->str:
	A=symbol;A=A.upper()
	if A in['VNINDEX','HNXINDEX','UPCOMINDEX','VN30','VN100','HNX30','VNSML','VNMID','VNALL','VNREAL','VNMAT','VNIT','VNHEAL','VNFINSELECT','VNFIN','VNENE','VNDIAMOND','VNCONS','VNCOND']:return'index'
	elif len(A)==3:return'stock'
	elif len(A)in[7,9]:
		B=re.compile('^VN30F\\d{1,2}M$');C=re.compile('^VN30F\\d{4}$');D=re.compile('^GB\\d{2}F\\d{4}$');E=re.compile('^(?!VN30F)[A-Z]{3}\\d{6}$')
		if D.match(A)or E.match(A):return'bond'
		elif B.match(A)or C.match(A):return'derivative'
		else:raise ValueError('Invalid derivative or bond symbol. Symbol must be in format of VN30F1M, VN30F2024, GB10F2024, or for company bonds, e.g., BAB122032')
	elif len(A)==8:return'coveredWarr'
	else:raise ValueError('Invalid symbol. Your symbol format is not recognized!')
def encode_url(string:str,safe:str=''):return urllib.parse.quote(string,safe=safe)
def days_between(start:str,end:str,format:str='%m/%d/%Y')->int:B=end;A=start;A=pd.to_datetime(A,format=format);B=pd.to_datetime(B,format=format);C=(B-A).days;return C
def lookback_date(period:str)->str:
	D=period
	try:
		A=D[-1].upper();B=int(D[:-1])
		if A=='D':C=datetime.now()-timedelta(days=B)
		elif A=='M':C=datetime.now()-timedelta(days=B*30)
		elif A=='Y':C=datetime.now()-timedelta(days=B*365)
		else:raise ValueError("Invalid period format. Use 'D', 'M', or 'Y' for days, months, or years.")
		return C.strftime('%Y-%m-%d')
	except Exception as E:raise ValueError(f"Error parsing period: {E}")
def filter_columns_by_language(df:pd.DataFrame,lang:str)->pd.DataFrame:
	H='_en';G='_vi';C=lang;B=df
	if C.lower()=='both':return B
	D=B.columns.tolist();I=[A for A in D if A.endswith(G)];J=[A for A in D if A.endswith(H)];K=[A for A in D if not(A.endswith(G)or A.endswith(H))]
	if C.lower()=='vi':E=K+I;A=B[E].copy();F={A:A.replace(G,'')for A in I};A=A.rename(columns=F)
	elif C.lower()=='en':E=K+J;A=B[E].copy();F={A:A.replace(H,'')for A in J};A=A.rename(columns=F)
	else:logger.warning(f"Invalid language parameter '{C}'. Use 'vi', 'en', or 'both'. Returning all columns.");return B
	return A
def vn_to_snake_case(text:str)->str:
	C='_';B='a';A=text;A=unicodedata.normalize('NFD',A);A=''.join([A for A in A if unicodedata.category(A)!='Mn']);D={'đ':'d','Đ':'d','ô':'o','Ô':'o','ư':'u','Ư':'u','ê':'e','Ê':'e','ă':B,'Ă':B,'â':B,'Â':B}
	for(E,F)in D.items():A=A.replace(E,F)
	A=A.lower();A=re.sub('[^a-z0-9]+',C,A);A=A.strip(C);A=re.sub('_+',C,A);return A