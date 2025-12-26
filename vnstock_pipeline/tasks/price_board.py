_F='symbol'
_E='update_time'
_D='update_unix'
_C=None
_B=False
_A=True
import time,os
from pathlib import Path
from datetime import datetime
import pandas as pd
from vnstock import Vnstock
from vnstock_pipeline.utils.deduplication import drop_duplicates
from vnstock_pipeline.template.vnstock import VNFetcher,VNValidator,VNTransformer
from vnstock_pipeline.utils.market_hours import trading_hours
DATA_DIR=Path('./data/price_board')
DATA_DIR.mkdir(parents=_A,exist_ok=_A)
OUTPUT=DATA_DIR/'price_board_transactions.csv'
class PriceBoardFetcher(VNFetcher):
	def _vn_call(B,ticker:str,**E)->pd.DataFrame:
		A=getattr(B,'tickers',_C)
		if not A:raise ValueError('Chưa thiết lập danh sách mã cho PriceBoardFetcher (self.tickers).')
		C=Vnstock().stock(symbol=A[0],source='VCI');D=C.trading.price_board(A,flatten_columns=_A,drop_levels=[0]);return D
class PriceBoardValidator(VNValidator):
	def validate(A,data:pd.DataFrame)->bool:
		if not isinstance(data,pd.DataFrame):print('Dữ liệu không phải là DataFrame.');return _B
		if _F not in data.columns:print("Thiếu cột 'symbol' trong dữ liệu Price Board.");return _B
		return _A
class PriceBoardTransformer(VNTransformer):
	def transform(C,data:pd.DataFrame)->pd.DataFrame:
		B='last_trading_date';A=data
		if B in A.columns:A[B]=pd.to_datetime(A[B],errors='coerce')
		A=drop_duplicates(A,subset=[_F]);return A
def append_with_timestamp(df:pd.DataFrame,path:Path):
	B=path;A=df;C=datetime.now();A[_D]=int(C.timestamp());A[_E]=C.strftime('%Y-%m-%d %H:%M:%S')
	if B.exists():A.to_csv(B,mode='a',header=_B,index=_B)
	else:A.to_csv(B,mode='w',header=_A,index=_B)
	print(f"Data appended at {A[_E].iloc[0]} (Unix: {A[_D].iloc[0]})")
def run_price_board(tickers:list,interval:int=60,mode:str='live'):
	I='ignore';H='dummy';G=tickers;from vnstock_pipeline.core.scheduler import Scheduler as J;B=PriceBoardFetcher();D=PriceBoardValidator();E=PriceBoardTransformer();B.tickers=G;K=J(B,D,E,_C,retry_attempts=1)
	if mode.lower()=='eod':
		print('Chế độ EOD: Lấy dữ liệu Price Board tĩnh một lần.');C=B.fetch(H)
		if not D.validate(C):print('Dữ liệu không hợp lệ, không lưu.')
		else:C=E.transform(C);append_with_timestamp(C,OUTPUT)
		print('EOD download hoàn thành.')
	else:
		print('Chế độ live: Cập nhật dữ liệu Price Board liên tục trong phiên giao dịch.');F=_C
		while _A:
			try:
				L=trading_hours(market='HOSE',enable_log=_A,language='en')
				if not L['is_trading_hour']:print('Ngoài giờ giao dịch. Dừng vòng lặp.');break
				K.run(G);A=B.fetch(H)
				if not D.validate(A):print('Dữ liệu không hợp lệ, bỏ qua lần cập nhật này.')
				else:
					A=E.transform(A)
					if F is not _C:
						M=A.drop(columns=[_D,_E],errors=I);N=F.drop(columns=[_D,_E],errors=I)
						if M.equals(N):print('Dữ liệu không thay đổi. Phiên giao dịch có thể đã kết thúc. Dừng vòng lặp.');break
					append_with_timestamp(A,OUTPUT);F=A.copy()
			except Exception as O:print(f"Error updating price board: {O}")
			time.sleep(interval)
if __name__=='__main__':TICKERS=['ACB','BCM','BID','BVH','CTG','FPT','GAS','GVR','HDB','HPG','LPB','MBB','MSN','MWG','PLX','SAB','SHB','SSB','SSI','STB','TCB','TPB','VCB','VHM','VIB','VIC','VJC','VNM','VPB','VRE'];run_price_board(TICKERS,interval=60,mode='live')