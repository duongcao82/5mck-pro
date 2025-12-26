import abc,logging,pandas as pd
from vnstock_pipeline.core.fetcher import Fetcher
from vnstock_pipeline.core.validator import Validator
from vnstock_pipeline.core.transformer import Transformer
logger=logging.getLogger(__name__)
class VNFetcher(Fetcher,abc.ABC):
	def __init__(A):0
	@abc.abstractmethod
	def _vn_call(self,ticker:str,**A)->pd.DataFrame:raise NotImplementedError
	def fetch(C,ticker:str,**D)->pd.DataFrame:
		A=ticker
		try:B=C._vn_call(A,**D);logger.debug(f"Lấy được {len(B)} bản ghi cho {A}.");return B
		except Exception as E:logger.error(f"Lỗi khi lấy dữ liệu cho {A}: {E}");raise
class VNValidator(Validator):
	required_columns=[]
	def validate(C,data:pd.DataFrame)->bool:
		B=False
		if not isinstance(data,pd.DataFrame):logger.error('Dữ liệu không phải là DataFrame.');return B
		A=[A for A in C.required_columns if A not in data.columns]
		if A:logger.warning(f"Thiếu các cột: {A}");return B
		return True
class VNTransformer(Transformer):
	def transform(E,data:pd.DataFrame)->pd.DataFrame:
		D='coerce';C='date';B='time';A=data.copy()
		if B in A.columns:A[B]=pd.to_datetime(A[B],errors=D);A=A.sort_values(B);A=A.reset_index(drop=True)
		elif C in A.columns:A[C]=pd.to_datetime(A[C],errors=D);A=A.sort_values(C);A=A.reset_index(drop=True)
		logger.debug('Đã chuyển đổi dữ liệu theo mặc định.');return A