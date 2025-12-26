from typing import Any
from tenacity import retry,stop_after_attempt,wait_exponential
from vnstock_data.config import Config
from vnstock_data.base import BaseAdapter,dynamic_method
class Company(BaseAdapter):
	_module_name='company'
	def __init__(B,source:str='vci',symbol:str=None,random_agent:bool=False,show_log:bool=False):
		A=source
		if A.lower()!='vci':raise ValueError("Lớp Company chỉ nhận giá trị tham số source là 'VCI'")
		super().__init__(source=A,symbol=symbol,random_agent=random_agent,show_log=show_log)
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def overview(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def shareholders(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def officers(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def subsidiaries(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def affiliate(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def news(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def events(self,*A:Any,**B:Any)->Any:0