_A=None
import logging,os,sys
from typing import Optional
def setup_logging(level:int=logging.INFO,log_file:Optional[str]='vnstock_pipeline.stream.log',console:bool=True,file_level:Optional[int]=_A,console_level:Optional[int]=_A)->_A:
	C=log_file;B=level;A=logging.getLogger();A.setLevel(B)
	for G in A.handlers[:]:A.removeHandler(G)
	H=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s');I=logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
	if C:
		D=os.path.dirname(C)
		if D and not os.path.exists(D):os.makedirs(D,exist_ok=True)
		E=logging.FileHandler(C);E.setLevel(file_level or B);E.setFormatter(H);A.addHandler(E)
	if console:F=logging.StreamHandler(sys.stdout);F.setLevel(console_level or B);F.setFormatter(I);A.addHandler(F)
	logging.info('Logging initialized')
def chunk_list(lst,chunk_size):A=chunk_size;return[lst[B:B+A]for B in range(0,len(lst),A)]
def safe_float(value,default=_A):
	try:return float(value)
	except(ValueError,TypeError):return default
def safe_int(value,default=_A):
	try:return int(value)
	except(ValueError,TypeError):return default