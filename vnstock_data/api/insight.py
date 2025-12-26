_A='VNINDEX'
from vnstock_data.base import BaseAdapter,dynamic_method
class TopStock(BaseAdapter):
	_module_name='insight';SUPPORTED_SOURCES=['vnd']
	def __init__(C,source:str='vnd',**D):
		A=source;B=A.lower()
		if B not in C.SUPPORTED_SOURCES:raise ValueError(f"Lớp TopStock chỉ nhận giá trị tham số source là 'VND'. Nhưng nhận được '{A}'.")
		super().__init__(source=B,**D)
	@dynamic_method
	def gainer(self,index:str=_A,limit:int=10):0
	@dynamic_method
	def loser(self,index:str=_A,limit:int=10):0
	@dynamic_method
	def value(self,index:str=_A,limit:int=10):0
	@dynamic_method
	def volume(self,index:str=_A,limit:int=10):0
	@dynamic_method
	def deal(self,index:str=_A,limit:int=10):0
	@dynamic_method
	def foreign_buy(self,date:str=None,limit:int=10):0
	@dynamic_method
	def foreign_sell(self,date:str=None,limit:int=10):0