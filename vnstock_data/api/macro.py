_D=False
_C='month'
_B='2025-04'
_A='2015-01'
from vnstock_data.base import BaseAdapter,dynamic_method
class Macro(BaseAdapter):
	_module_name='macro'
	def __init__(B,source:str='mbk',random_agent:bool=_D,show_log:bool=_D):
		A=source
		if A!='mbk':raise ValueError('Lớp Macro không hỗ trợ thay đổi tham số source.')
		super().__init__(source=A,random_agent=random_agent,show_log=show_log)
	@dynamic_method
	def gdp(self,start:str=_A,end:str=_B,period:str='quarter',keep_label:bool=_D):0
	@dynamic_method
	def cpi(self,start:str=_A,end:str=_B,period:str=_C):0
	@dynamic_method
	def industry_prod(self,start:str=_A,end:str=_B,period:str=_C):0
	@dynamic_method
	def import_export(self,start:str=_A,end:str=_B,period:str=_C):0
	@dynamic_method
	def retail(self,start:str=_A,end:str=_B,period:str=_C):0
	@dynamic_method
	def fdi(self,start:str=_A,end:str=_B,period:str=_C):0
	@dynamic_method
	def money_supply(self,start:str=_A,end:str=_B,period:str=_C):0
	@dynamic_method
	def exchange_rate(self,start:str='2025-01-02',end:str='2025-04-10',period:str='day'):0
	@dynamic_method
	def population_labor(self,start:str=_A,end:str=_B,period:str='year'):0