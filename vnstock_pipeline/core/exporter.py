_I='parquet'
_H='snappy'
_G='%Y-%m-%d'
_F='intraday'
_E='time'
_D='csv'
_C=False
_B=True
_A=None
import abc,os
from datetime import datetime
from pathlib import Path
from typing import List,Optional
import pandas as pd
class Exporter(abc.ABC):
	@abc.abstractmethod
	def export(self,data,ticker:str,**A):0
	def preview(A,ticker:str,n:int=5,**B):0
BaseExporter=Exporter
class CSVExport(Exporter):
	def __init__(A,base_path:str):
		A.base_path=base_path
		if not os.path.exists(A.base_path):os.makedirs(A.base_path)
	def export(D,data,ticker:str,**G):
		C=data;A=os.path.join(D.base_path,f"{ticker}.csv")
		if os.path.exists(A):
			try:
				E=pd.read_csv(A);B=pd.concat([E,C],ignore_index=_B)
				if _E in B.columns and'id'in B.columns:B=B.drop_duplicates(subset=[_E,'id'])
				B.to_csv(A,index=_C)
			except Exception as F:print(f"[CSVExport] Lỗi khi đọc/gộp dữ liệu cũ: {F}. Ghi append như cũ.");C.to_csv(A,mode='a',header=_C,index=_C)
		else:C.to_csv(A,index=_C)
	def preview(B,ticker:str,n:int=5,**D):
		A=os.path.join(B.base_path,f"{ticker}.csv")
		if not os.path.exists(A):return
		C=pd.read_csv(A);return C.tail(n)
class ParquetExport(Exporter):
	def __init__(A,base_path:str,data_type:str='stock_data'):A.base_path=Path(base_path);A.data_type=data_type;A.base_path.mkdir(parents=_B,exist_ok=_B)
	def _get_file_path(A,ticker:str,date:Optional[str]=_A)->Path:C=date or datetime.now().strftime(_G);B=A.base_path/A.data_type/C;B.mkdir(parents=_B,exist_ok=_B);return B/f"{ticker}.parquet"
	def export(A,data,ticker:str,date:Optional[str]=_A,**F):
		try:import pyarrow as B,pyarrow.parquet as C
		except ImportError:raise ImportError('pyarrow is required for ParquetExport. Install it with: pip install pyarrow')
		D=A._get_file_path(ticker,date);E=B.Table.from_pandas(data);C.write_table(E,D,compression=_H,use_dictionary=_B,version='2.6',data_page_size=1048576)
	def preview(C,ticker:str,n:int=5,date:Optional[str]=_A,**G):
		try:import pyarrow.parquet as D
		except ImportError:raise ImportError('pyarrow is required for ParquetExport')
		A=C._get_file_path(ticker,date)
		if not A.exists():return
		B=D.ParquetFile(A);H=B.metadata.num_rows;E=B.read_row_groups(row_groups=[0],columns=_A,use_threads=_B);F=E.to_pandas();return F.tail(n)
class DuckDBExport(Exporter):
	def __init__(A,db_path:str):A.db_path=db_path
	def export(D,data,ticker:str,**F):
		C='data';B=ticker
		try:import duckdb as E
		except ImportError:raise ImportError('duckdb is required for DuckDBExport. Install it with: pip install duckdb')
		A=E.connect(D.db_path);A.execute(f"CREATE TABLE IF NOT EXISTS {B} AS SELECT * FROM data LIMIT 0",{C:data});A.execute(f"INSERT INTO {B} SELECT * FROM data",{C:data});A.close()
class TimeSeriesExporter(Exporter):
	def __init__(A,base_path:str,file_format:str=_D,dedup_columns:Optional[List[str]]=_A)->_A:
		A.base_path=Path(base_path);A.file_format=file_format.lower();A.dedup_columns=dedup_columns or[_E,'ticker']
		if A.file_format not in[_D,_I]:B="file_format must be 'csv' or 'parquet'";raise ValueError(B)
		A.base_path.mkdir(parents=_B,exist_ok=_B)
	def _build_path(B,ticker:str,data_type:str,date:Optional[str]=_A,subfolder:Optional[str]=_A)->Path:
		C=subfolder;E=date or datetime.now().strftime(_G);F=_D if B.file_format==_D else _I;A=[B.base_path,data_type]
		if C:A.append(C)
		A.append(E);D=Path(*A);D.mkdir(parents=_B,exist_ok=_B);return D/f"{ticker}.{F}"
	def _read_file(B,file_path:Path)->pd.DataFrame:
		A=file_path
		if B.file_format==_D:return pd.read_csv(A)
		else:return pd.read_parquet(A)
	def _write_file(B,file_path:Path,data:pd.DataFrame)->_A:
		A=file_path
		if B.file_format==_D:data.to_csv(A,index=_C)
		else:data.to_parquet(A,engine='pyarrow',compression=_H,index=_C)
	def _deduplicate(C,data:pd.DataFrame)->pd.DataFrame:
		A=data;B=[B for B in C.dedup_columns if B in A.columns]
		if not B:return A
		return A.drop_duplicates(subset=B,keep='last')
	def export(A,data:pd.DataFrame,ticker:str,data_type:str=_F,date:Optional[str]=_A,append_mode:bool=_B,deduplicate:bool=_C,subfolder:Optional[str]=_A,**G):
		E=deduplicate;B=data
		if B is _A or B.empty:return
		C=A._build_path(ticker,data_type,date,subfolder)
		if C.exists()and append_mode:
			F=A._read_file(C);D=pd.concat([F,B],ignore_index=_B)
			if E:D=A._deduplicate(D)
			A._write_file(C,D)
		else:
			if E:B=A._deduplicate(B)
			A._write_file(C,B)
		return C
	def preview(A,ticker:str,n:int=5,data_type:str=_F,date:Optional[str]=_A,subfolder:Optional[str]=_A,**D):
		B=A._build_path(ticker,data_type,date,subfolder)
		if not B.exists():return
		C=A._read_file(B);return C.tail(n)
	def read_all(A,ticker:str,data_type:str=_F,date:Optional[str]=_A,subfolder:Optional[str]=_A)->Optional[pd.DataFrame]:
		B=A._build_path(ticker,data_type,date,subfolder)
		if not B.exists():return
		return A._read_file(B)
	def list_dates(C,ticker:str,data_type:str,subfolder:Optional[str]=_A)->List[str]:
		E=subfolder;D=ticker;A=C.base_path/data_type
		if E:A=A/E
		if not A.exists():return[]
		F=[];G=f"{D}.csv"if C.file_format==_D else f"{D}.parquet"
		for B in A.iterdir():
			if B.is_dir()and(B/G).exists():F.append(B.name)
		return sorted(F)
	def read_date_range(B,ticker:str,data_type:str,start_date:str,end_date:str,subfolder:Optional[str]=_A)->pd.DataFrame:
		E=subfolder;D=data_type;C=ticker;H=B.list_dates(C,D,E);F=[A for A in H if start_date<=A<=end_date]
		if not F:return pd.DataFrame()
		A=[]
		for I in F:
			G=B.read_all(C,D,I,E)
			if G is not _A:A.append(G)
		if not A:return pd.DataFrame()
		return pd.concat(A,ignore_index=_B)