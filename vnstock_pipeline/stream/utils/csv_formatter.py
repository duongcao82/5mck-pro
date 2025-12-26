import os,pandas as pd,logging
from typing import Optional
logger=logging.getLogger(__name__)
def format_csv_with_timestamp_index(csv_path:str,output_path:Optional[str]=None)->bool:
	F=False;D='timestamp';C=True;B=csv_path
	try:
		if not os.path.exists(B):logger.error(f"CSV file not found: {B}");return F
		A=pd.read_csv(B)
		if A.empty:logger.warning(f"CSV is empty: {B}");return C
		if D not in A.columns:logger.warning(f"No timestamp column in {B}");return C
		G=A.pop(D);A.insert(0,D,G);A=A.set_index(D);E=output_path or B;os.makedirs(os.path.dirname(E)or'.',exist_ok=C);A.to_csv(E);logger.info(f"Formatted CSV: {B} ({len(A.columns)} columns, {len(A)} rows)");return C
	except Exception as H:logger.error(f"Error formatting CSV {B}: {H}");return F
def format_all_csvs_in_directory(directory:str,pattern:str='market_data_*.csv')->int:
	A=directory
	try:
		import glob;C=glob.glob(os.path.join(A,pattern));B=0
		for D in C:
			if format_csv_with_timestamp_index(D):B+=1
		logger.info(f"Formatted {B}/{len(C)} CSV files in {A}");return B
	except Exception as E:logger.error(f"Error formatting CSVs in {A}: {E}");return 0