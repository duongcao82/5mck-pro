import pandas as pd
from typing import Union
from vnstock_ta.indicators.trend import TrendIndicator
from vnstock_ta.indicators.momentum import MomentumIndicator
from vnstock_ta.indicators.volatility import VolatilityIndicator
from vnstock_ta.indicators.volume import VolumeIndicator
from vnstock_ta.chart.trend import TATrend
from vnstock_ta.chart.momentum import TAMomentum
from vnstock_ta.chart.volatility import TAVolatility
from vnstock_ta.chart.volume import TAVolume
from vnstock_ta.utils.const import _ISLAND_GREEN,_ORANGE,_TURKISH_SEA,_SLATE_BLUE,_GRADIENT_EMERALD,NEUTRAL_INFORMATION_COMPLETE,DARK_MODE_PRIMARY_COLORS,DARK_MODE_SECONDARY_COLORS,LIGHT_MODE_PRIMARY_COLORS,LIGHT_MODE_SECONDARY_COLORS
from vnstock_ta.indicators.docs import*
class Indicator:
	def __init__(self,data:pd.DataFrame):self.data=data;self.trend=TrendIndicator(data);self.momentum=MomentumIndicator(data);self.volatility=VolatilityIndicator(data);self.volume=VolumeIndicator(data);self._bind_methods()
	def _bind_methods(self):
		components=[self.trend,self.momentum,self.volatility,self.volume]
		for component in components:
			for attr_name in dir(component):
				if callable(getattr(component,attr_name))and not attr_name.startswith('_'):setattr(self,attr_name,getattr(component,attr_name))
	def __getattr__(self,name):raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
class Plotter:
	def __init__(self,data:pd.DataFrame,theme:str='light',watermark:bool=False,display:bool=True):self.data=data;self.theme=theme;self.trend=TATrend(data,theme,watermark,display);self.momentum=TAMomentum(data,theme,watermark,display);self.volatility=TAVolatility(data,theme,watermark,display);self.volume=TAVolume(data,theme,watermark,display);self._bind_methods()
	def _bind_methods(self):
		components=[self.trend,self.momentum,self.volatility,self.volume]
		for component in components:
			for attr_name in dir(component):
				if callable(getattr(component,attr_name))and not attr_name.startswith('_'):setattr(self,attr_name,getattr(component,attr_name))
	def __getattr__(self,name):raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")