from typing import Any
from tenacity import retry,stop_after_attempt,wait_exponential
from vnstock_data.config import Config
from vnstock_data.base import BaseAdapter,dynamic_method
class Quote(BaseAdapter):
	_module_name='quote'
	def __init__(B,source:str='vci',symbol:str='',random_agent:bool=False,show_log:bool=False):
		A=source
		if A.lower()not in['vci','vnd','mas']:raise ValueError("Lớp Quote chỉ nhận giá trị tham số source là 'VCI' hoặc 'VND'.")
		super().__init__(source=A,symbol=symbol,random_agent=random_agent,show_log=show_log)
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def history(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def intraday(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def price_depth(self,*A:Any,**B:Any)->Any:0