from typing import Any
from tenacity import retry,stop_after_attempt,wait_exponential
from vnstock_data.config import Config
from vnstock_data.base import BaseAdapter,dynamic_method
class Listing(BaseAdapter):
	_module_name='listing'
	def __init__(B,source:str='vci',random_agent:bool=False,show_log:bool=False):
		A=source
		if A.lower()not in['vci','vnd']:raise ValueError("Lớp Listing chỉ nhận giá trị tham số source là 'VCI' hoặc 'VND'.")
		super().__init__(source=A,random_agent=random_agent,show_log=show_log)
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def all_symbols(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def symbols_by_industries(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def symbols_by_exchange(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def industries_icb(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	@dynamic_method
	def symbols_by_group(self,*A:Any,**B:Any)->Any:0
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	def all_future_indices(self,**A:Any)->Any:return self.symbols_by_group(group='FU_INDEX',**A)
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	def all_government_bonds(self,**A:Any)->Any:return self.symbols_by_group(group='FU_BOND',**A)
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	def all_covered_warrant(self,**A:Any)->Any:return self.symbols_by_group(group='CW',**A)
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	def all_bonds(self,**A:Any)->Any:return self.symbols_by_group(group='BOND',**A)