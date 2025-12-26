_H='page_size'
_G='live'
_F='append'
_E=False
_D='backup'
_C='id'
_B=True
_A='time'
import time,os,shutil
from pathlib import Path
from datetime import datetime
import pandas as pd
from vnstock import Vnstock
from vnstock_data.explorer.vci import Quote
from vnstock_pipeline.template.vnstock import VNFetcher,VNValidator,VNTransformer
from vnstock_pipeline.utils.deduplication import drop_duplicates
from vnstock_pipeline.utils.market_hours import trading_hours
from vnstock_pipeline.core.exporter import Exporter
class IntradayFetcher(VNFetcher):
	def _vn_call(E,ticker:str,**A)->pd.DataFrame:B=A.get(_H,50000);C=Quote(symbol=ticker);D=C.intraday(page_size=B);return D
class IntradayValidator(VNValidator):required_columns=[_A,'price','volume','match_type',_C]
class IntradayTransformer(VNTransformer):
	def transform(B,data:pd.DataFrame)->pd.DataFrame:
		A=super().transform(data);A=drop_duplicates(A,subset=[_A,_C])
		if _A in A.columns:A[_A]=pd.to_datetime(A[_A])
		A=A.sort_values(_A);return A
class SmartCSVExport(Exporter):
	def __init__(A,base_path:str,backup_dir:str=None,max_backups:int=2):
		B=backup_dir;A.base_path=Path(base_path);A.base_path.mkdir(parents=_B,exist_ok=_B)
		if B is None:A.backup_dir=A.base_path/_D
		else:A.backup_dir=Path(B)
		A.backup_dir.mkdir(parents=_B,exist_ok=_B);A.max_backups=max_backups
	def _get_file_path(A,ticker:str)->Path:return A.base_path/f"{ticker}.csv"
	def _cleanup_old_backups(A,ticker:str):
		B=list(A.backup_dir.glob(f"{ticker}_*.csv"));B.sort(key=lambda x:x.stat().st_mtime,reverse=_B)
		if len(B)>A.max_backups:
			for C in B[A.max_backups:]:
				try:C.unlink();print(f"Xóa file backup cũ: {C.name}")
				except Exception as D:print(f"Không thể xóa file backup cũ {C.name}: {D}")
	def _backup_file(A,ticker:str)->bool:
		B=ticker;C=A._get_file_path(B)
		if not C.exists():return _E
		D=datetime.now().strftime('%Y%m%d_%H%M%S');E=A.backup_dir/f"{B}_{D}.csv";shutil.copy2(C,E);A._cleanup_old_backups(B);return _B
	def _save_atomic(B,df:pd.DataFrame,path:Path):A=path.with_suffix('.csv.tmp');df.to_csv(A,index=_E);os.replace(A,path)
	def _read_existing_data(C,ticker:str)->pd.DataFrame:
		B=C._get_file_path(ticker)
		if B.exists():
			try:
				A=pd.read_csv(B)
				if _A in A.columns:A[_A]=pd.to_datetime(A[_A])
				return A
			except Exception as D:print(f"Lỗi khi đọc file {B}: {D}");return pd.DataFrame()
		return pd.DataFrame()
	def export(B,data:pd.DataFrame,ticker:str,**F):
		C=ticker;A=data;H=F.get('mode',_F);I=F.get(_D,_B);D=B._get_file_path(C)
		if A is None or A.empty:print(f"Không có dữ liệu mới cho {C}");return
		if _A in A.columns:A[_A]=pd.to_datetime(A[_A])
		if H==_F and D.exists():
			if I:B._backup_file(C)
			E=B._read_existing_data(C)
			if E.empty:B._save_atomic(A,D);print(f"[{C}] Đã lưu dữ liệu intraday mới");return
			G=B._smart_append(E,A);B._save_atomic(G,D);print(f"[{C}] Đã cập nhật dữ liệu intraday (từ {len(E)} đến {len(G)} dòng)")
		else:B._save_atomic(A,D);print(f"[{C}] Đã lưu dữ liệu intraday ({len(A)} dòng)")
	def _smart_append(H,old_data:pd.DataFrame,new_data:pd.DataFrame)->pd.DataFrame:
		B=new_data;A=old_data
		if A.empty:return B
		if B.empty:return A
		for D in[A,B]:
			if _A not in D.columns:raise ValueError("DataFrame phải có cột 'time'")
			D[_A]=pd.to_datetime(D[_A])
		A=A.sort_values(_A);B=B.sort_values(_A)
		if not B.empty:
			E=B[_A].min();F=E.replace(second=0,microsecond=0);G=A[A[_A]<F];C=pd.concat([G,B])
			if _C in C.columns:C=drop_duplicates(C,subset=[_A,_C])
			else:C=drop_duplicates(C,subset=[_A])
			C=C.sort_values(_A).reset_index(drop=_B);return C
		else:return A
	def preview(C,ticker:str,n:int=5,**E):
		B=C._get_file_path(ticker)
		if not B.exists():return
		try:
			A=pd.read_csv(B)
			if _A in A.columns:A[_A]=pd.to_datetime(A[_A]);A=A.sort_values(_A,ascending=_E)
			return A.head(n)
		except Exception as D:print(f"Lỗi khi đọc file {B}: {D}");return
def run_intraday_task(tickers:list,interval:int=60,mode:str=_G,page_size:int=50000,backup:bool=_B,max_backups:int=2):
	M='trading_session';L='./data/intraday';F=interval;E=tickers;from vnstock_pipeline.core.scheduler import Scheduler as G;from vnstock_pipeline.core.exporter import CSVExport as N;H=IntradayFetcher();I=IntradayValidator();J=IntradayTransformer();K={_H:page_size}
	if mode.lower()=='eod':print('Chế độ EOD: Lấy dữ liệu intraday tĩnh một lần.');A=N(base_path=L);B={};C=G(H,I,J,A);C.run(E,fetcher_kwargs=K,exporter_kwargs=B);print('EOD download hoàn thành.')
	else:
		print('Chế độ live: Cập nhật dữ liệu intraday liên tục trong phiên giao dịch.');A=SmartCSVExport(base_path=L,max_backups=max_backups);B={'mode':_F,_D:backup};C=G(H,I,J,A)
		try:
			while _B:
				try:
					D=trading_hours(market='HOSE',enable_log=_B,language='vi')
					if not D['is_trading_hour']:print(f"Ngoài giờ giao dịch ({D[M]}). Đợi 5 phút...");time.sleep(300);continue
					print(f"Đang cập nhật dữ liệu trong phiên {D[M]}...");C.run(E,fetcher_kwargs=K,exporter_kwargs=B);print(f"Hoàn thành cập nhật. Đợi {F} giây trước khi cập nhật tiếp...")
				except Exception as O:print(f"Lỗi khi cập nhật dữ liệu intraday: {O}")
				time.sleep(F)
		except KeyboardInterrupt:print('Đã dừng cập nhật dữ liệu theo yêu cầu.')
if __name__=='__main__':sample_tickers=['ACB','VCB','HPG'];mode=_G;page_size=5000 if mode==_G else 50000;run_intraday_task(sample_tickers,interval=60,mode=mode,page_size=page_size,backup=_B,max_backups=2)