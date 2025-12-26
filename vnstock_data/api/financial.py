_A='quarter'
from typing import Any
from tenacity import retry,stop_after_attempt,wait_exponential
from vnstock_data.config import Config
from vnstock_data.base import BaseAdapter,dynamic_method
class Finance(BaseAdapter):
	_module_name='financial'
	def __init__(C,source:str,symbol:str,period:str=_A,get_all:bool=True,show_log:bool=False):
		B=period;A=source
		if A.lower()not in['vci','mas']:raise ValueError("Lớp Finance chỉ nhận giá trị tham số source là 'VCI' hoặc 'MAS'.")
		if B.lower()not in['year',_A]:raise ValueError("Lớp Finance chỉ nhận giá trị tham số period là 'year' hoặc 'quarter'.")
		super().__init__(source=A,symbol=symbol,period=B,get_all=get_all,show_log=show_log)
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def balance_sheet(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def income_statement(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def cash_flow(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def note(self,*A:Any,**B:Any)->Any:
		if self.source.lower()=='vci':return self._provider.note(*A,**B)
		raise NotImplementedError("'note' method is only implemented for source 'vci'.")
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def ratio(self,*A:Any,**B:Any)->Any:0