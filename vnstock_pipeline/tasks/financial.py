_E='quarter'
_D=False
_C=True
_B=None
_A='year'
import os
from pathlib import Path
import pandas as pd
from vnstock_data.explorer.vci import Finance
from vnstock_pipeline.template.vnstock import VNFetcher,VNValidator,VNTransformer
class FinancialFetcher(VNFetcher):
	def _vn_call(R,ticker:str,**C)->dict:
		L='ratio';K='cash_flow';J='income_statement_quarter';I='income_statement_year';H='balance_sheet';D=ticker;E=Finance(symbol=D);M=C.get('balance_sheet_period',_A);N=C.get('income_statement_year_period',_A);O=C.get('income_statement_quarter_period',_E);P=C.get('cash_flow_period',_A);Q=C.get('ratio_period',_A);F=C.get('lang','vi');G=C.get('dropna',_C);A={}
		try:A[H]=E.balance_sheet(period=M,lang=F,dropna=G)
		except Exception as B:A[H]=_B;print(f"Lỗi khi lấy balance_sheet cho {D}: {B}")
		try:A[I]=E.income_statement(period=N,lang=F,dropna=G)
		except Exception as B:A[I]=_B;print(f"Lỗi khi lấy income_statement_year cho {D}: {B}")
		try:A[J]=E.income_statement(period=O,lang=F,dropna=G)
		except Exception as B:A[J]=_B;print(f"Lỗi khi lấy income_statement_quarter cho {D}: {B}")
		try:A[K]=E.cash_flow(period=P,lang=F,dropna=G)
		except Exception as B:A[K]=_B;print(f"Lỗi khi lấy cash_flow cho {D}: {B}")
		try:A[L]=E.ratio(period=Q,lang=F,dropna=G)
		except Exception as B:A[L]=_B;print(f"Lỗi khi lấy ratio cho {D}: {B}")
		return A
class FinancialValidator(VNValidator):
	def validate(B,data:dict)->bool:
		if not isinstance(data,dict):print('Dữ liệu không phải là dictionary.');return _D
		for(C,A)in data.items():
			if A is not _B and isinstance(A,pd.DataFrame)and not A.empty:return _C
		print('Không có báo cáo tài chính hợp lệ nào được lấy.');return _D
class FinancialTransformer(VNTransformer):
	def transform(A,data:dict)->dict:return data
class FinancialExporter:
	def __init__(A,base_path:str):A.base_path=base_path;Path(A.base_path).mkdir(parents=_C,exist_ok=_C)
	def export(E,data,ticker:str,**F):
		B=ticker
		for(C,A)in data.items():
			if A is not _B and not A.empty:D=os.path.join(E.base_path,f"{B}_{C}.csv");A.to_csv(D,index=_D);print(f"Đã lưu {C} cho {B} vào {D}")
	def preview(A,ticker:str,n:int=5,**B):0
def run_financial_task(tickers:list,**B):
	from vnstock_pipeline.core.scheduler import Scheduler as C;A=FinancialFetcher();D=FinancialValidator();E=FinancialTransformer();F=FinancialExporter(base_path='./data/financial');A.params=B;G=A.fetch
	def H(ticker:str):return G(ticker,**A.params or{})
	A.fetch=H;I=C(A,D,E,F,retry_attempts=1);I.run(tickers)
if __name__=='__main__':sample_tickers=['ACB','VCB','HPG'];run_financial_task(sample_tickers,balance_sheet_period=_A,income_statement_year_period=_A,income_statement_quarter_period=_E,cash_flow_period=_A,ratio_period=_A,lang='vi',dropna=_C)