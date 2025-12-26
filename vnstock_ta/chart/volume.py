_A=False
import pandas as pd
from pyecharts import options as opts
from vnstock_ta.utils.const import _ISLAND_GREEN,_ORANGE,_TURKISH_SEA,_SLATE_BLUE,_LIME_PUNCH,_GRADIENT_EMERALD,NEUTRAL_INFORMATION_COMPLETE,DARK_MODE_PRIMARY_COLORS,DARK_MODE_SECONDARY_COLORS,LIGHT_MODE_PRIMARY_COLORS,LIGHT_MODE_SECONDARY_COLORS
from vnstock_ta.chart.core import TAChart
class TAVolume(TAChart):
	def __init__(A,data:pd.DataFrame,theme:str='light',watermark:bool=True,display:bool=True):super().__init__(data,theme,watermark,display)
	def obv(A,title='On-Balance Volume',color=_ISLAND_GREEN,legend=_A,watermark=True,minimal:bool=_A):
		F='subplot';C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		G=A.ta.obv().round(2);H=A.data.index.strftime('%Y-%m-%d').tolist();E=A.chart._line(time_series=H,data_series=G,color=D,title=B,yaxis_name='OBV',is_smooth=_A,legend=legend,watermark=watermark,show_xaxis=_A)
		if minimal:return A._minimal_chart(indicator_chart=E,title=B,layout=F)
		else:return A._base_chart(indicator_chart=E,title=B,layout=F)