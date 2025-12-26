_A=None
from vnstock_data.base import BaseAdapter,dynamic_method
class CommodityPrice(BaseAdapter):
	_module_name='commodity'
	def __init__(B,source:str='spl',start:str=_A,end:str=_A,show_log:bool=False):
		A=source
		if A.lower()!='spl':raise ValueError("Lớp Commodity chỉ nhận giá trị tham số source là 'SPL'.")
		super().__init__(source=A,symbol=_A,start=start,end=end,show_log=show_log)
	@dynamic_method
	def gold_vn(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def gold_global(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def gas_vn(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def oil_crude(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def gas_natural(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def coke(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def steel_d10(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def iron_ore(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def steel_hrc(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def fertilizer_ure(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def soybean(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def corn(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def sugar(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def pork_north_vn(self,start:str=_A,end:str=_A):0
	@dynamic_method
	def pork_china(self,start:str=_A,end:str=_A):0