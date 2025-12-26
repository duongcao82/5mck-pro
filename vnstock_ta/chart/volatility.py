_E='rgba(167, 202, 241, 0.1)'
_D='subplot'
_C='%Y-%m-%d'
_B=True
_A=False
import pandas as pd
from pyecharts import options as opts
from vnstock_ta.utils.const import _ISLAND_GREEN,_ORANGE,_TURKISH_SEA,_SLATE_BLUE,_LIME_PUNCH,_GRADIENT_EMERALD,NEUTRAL_INFORMATION_COMPLETE,DARK_MODE_PRIMARY_COLORS,DARK_MODE_SECONDARY_COLORS,LIGHT_MODE_PRIMARY_COLORS,LIGHT_MODE_SECONDARY_COLORS
from vnstock_ta.chart.core import TAChart
class TAVolatility(TAChart):
	def __init__(A,data:pd.DataFrame,theme:str='light',watermark:bool=_B,display:bool=_B):super().__init__(data,theme,watermark,display)
	def bbands(A,length:int=10,std:int=2,title:str='Bollinger Bands',color=[_TURKISH_SEA,_ORANGE],legend=_B,watermark=_B,minimal:bool=_A):
		C=color;B=title
		if C:D=C[0];G=C[1]
		else:D=_E;G=_ORANGE
		E=A.ta.bbands(length=length,std=std).round(2);I=E.iloc[:,0];J=E.iloc[:,1];K=E.iloc[:,2];L=A.data.index.strftime(_C).tolist();F=A.chart._line(time_series=L,data_series=K,color=D,area_style=_A,title=B,yaxis_name='Up Band',legend=legend,watermark=watermark);F=A.chart._add_line(line_chart=F,data_series=I,color=D,title=B,yaxis_name='Low Band');H=A.chart._add_line(line_chart=F,data_series=J,color=G,title=B,yaxis_name='Mid Band')
		if minimal:return A._minimal_chart(indicator_chart=H,title=B)
		else:return A._base_chart(indicator_chart=H,title=B)
	def kc(A,length:int=20,scalar:int=2,mamode:str='ema',title:str='Keltner Channels',color=[_TURKISH_SEA,_ORANGE],legend=_B,watermark=_B,minimal:bool=_A):
		C=color;B=title
		if C:D=C[0];G=C[1]
		else:D=_E;G=_ORANGE
		E=A.ta.kc(length=length,scalar=scalar,mamode=mamode).round(2);I=E.iloc[:,0];J=E.iloc[:,1];K=E.iloc[:,2];L=A.data.index.strftime(_C).tolist();F=A.chart._line(time_series=L,data_series=K,color=D,area_style=_A,title=B,yaxis_name='Up',legend=legend,watermark=watermark);F=A.chart._add_line(line_chart=F,data_series=I,color=D,title=B,yaxis_name='Low');H=A.chart._add_line(line_chart=F,data_series=J,color=G,title=B,yaxis_name='Mid')
		if minimal:return A._minimal_chart(indicator_chart=H,title=B)
		else:return A._base_chart(indicator_chart=H,title=B)
	def atr(A,length:int=14,title='Average True Range',color=_ISLAND_GREEN,legend=_A,watermark=_B,minimal:bool=_A):
		C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		F=A.ta.atr(length=length).round(2);G=A.data.index.strftime(_C).tolist();E=A.chart._line(time_series=G,data_series=F,color=D,title=B,yaxis_name='ATR',is_smooth=_A,legend=legend,watermark=watermark,show_xaxis=_A)
		if minimal:return A._minimal_chart(indicator_chart=E,title=B,layout=_D)
		else:return A._base_chart(indicator_chart=E,title=B,layout=_D)
	def stdev(A,length:int=14,ddof=1,title='Standard Deviation',color=_ISLAND_GREEN,legend=_A,watermark=_B,minimal:bool=_A):
		C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		F=A.ta.stdev(length=length,ddof=ddof).round(2);G=A.data.index.strftime(_C).tolist();E=A.chart._line(time_series=G,data_series=F,color=D,title=B,yaxis_name='STDEV',is_smooth=_A,legend=legend,watermark=watermark,show_xaxis=_A)
		if minimal:return A._minimal_chart(indicator_chart=E,title=B,layout=_D)
		else:return A._base_chart(indicator_chart=E,title=B,layout=_D)
	def linreg(A,length:int=10,title:str='Linear Regression Curve',color=_ORANGE,legend=_B,watermark=_B,minimal:bool=_A):
		C=color;B=title
		if C:E=C
		else:E=A.chart.mono_color
		if A.theme=='dark':F=list(DARK_MODE_PRIMARY_COLORS.values())+list(DARK_MODE_SECONDARY_COLORS.values())
		else:F=list(LIGHT_MODE_PRIMARY_COLORS.values())+list(LIGHT_MODE_SECONDARY_COLORS.values())
		G=A.data.index.strftime(_C).tolist();H=A.ta.sma(length=length).round(2);D=A.chart._line(time_series=G,data_series=H,color=_ORANGE,title=B,yaxis_name='LR',legend=legend,watermark=watermark)
		if minimal:return A._minimal_chart(indicator_chart=D,title=B)
		else:return A._base_chart(indicator_chart=D,title=B)