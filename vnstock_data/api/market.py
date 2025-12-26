from vnstock_data.base import BaseAdapter,dynamic_method
class Market(BaseAdapter):
	_module_name='market';SUPPORTED_SOURCES=['vnd']
	def __init__(C,source:str='vnd',**D):
		A=source;B=A.lower()
		if B not in C.SUPPORTED_SOURCES:raise ValueError(f"Lớp Market chỉ nhận giá trị tham số source là 'VND'. Nhưng nhận được '{A}'.")
		super().__init__(source=B,**D)
	@dynamic_method
	def pe(self,duration:str='5Y'):0
	@dynamic_method
	def pb(self,duration:str='5Y'):0
	@dynamic_method
	def evaluation(self,duration:str='5Y'):0