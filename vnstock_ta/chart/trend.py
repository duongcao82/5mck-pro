_E='subplot'
_D='dark'
_C='%Y-%m-%d'
_B=False
_A=True
import pandas as pd
from pyecharts import options as opts
from vnstock_ta.utils.const import _ISLAND_GREEN,_ORANGE,_TURKISH_SEA,_SLATE_BLUE,_GRADIENT_EMERALD,NEUTRAL_INFORMATION_COMPLETE,DARK_MODE_PRIMARY_COLORS,DARK_MODE_SECONDARY_COLORS,LIGHT_MODE_PRIMARY_COLORS,LIGHT_MODE_SECONDARY_COLORS
from vnstock_ta.chart.core import TAChart
class TATrend(TAChart):
	def __init__(A,data:pd.DataFrame,theme:str='light',watermark:bool=_A,display:bool=_A):super().__init__(data,theme,watermark,display)
	def sma(A,length:int=10,title:str='Simple Moving Average',color=_ORANGE,legend=_A,watermark=_A,minimal:bool=_B):
		J=watermark;I=legend;H=color;D=title;B=length
		if H:F=H
		else:F=A.chart.mono_color
		if A.theme==_D:K=list(DARK_MODE_PRIMARY_COLORS.values())+list(DARK_MODE_SECONDARY_COLORS.values())
		else:K=list(LIGHT_MODE_PRIMARY_COLORS.values())+list(LIGHT_MODE_SECONDARY_COLORS.values())
		L=A.data.index.strftime(_C).tolist()
		if isinstance(B,list):
			G=A.ta.sma(length=B[0]).round(2);C=A.chart._line(time_series=L,data_series=G,color=F,title=D,yaxis_name=f"SMA{B[0]}",legend=I,watermark=J)
			for E in range(1,len(B)):M=A.ta.sma(length=B[E]);C=A.chart._add_line(line_chart=C,data_series=M,color=K[E],title=f"SMA - {B[E]} kỳ",yaxis_name=f"SMA{B[E]}")
		else:G=A.ta.sma(length=B).round(2);C=A.chart._line(time_series=L,data_series=G,color=F,title=D,yaxis_name='SMA',legend=I,watermark=J)
		if minimal:return A._minimal_chart(indicator_chart=C,title=D)
		else:return A._base_chart(indicator_chart=C,title=D)
	def ema(A,length:int=10,title:str='Exponential Moving Average',color=_ORANGE,legend=_A,watermark=_A,minimal:bool=_B):
		J=watermark;I=legend;H=color;D=title;B=length
		if H:F=H
		else:F=A.chart.mono_color
		if A.theme==_D:K=list(DARK_MODE_PRIMARY_COLORS.values())+list(DARK_MODE_SECONDARY_COLORS.values())
		else:K=list(LIGHT_MODE_PRIMARY_COLORS.values())+list(LIGHT_MODE_SECONDARY_COLORS.values())
		L=A.data.index.strftime(_C).tolist()
		if isinstance(B,list):
			G=A.ta.ema(length=B[0]).round(2);C=A.chart._line(time_series=L,data_series=G,color=F,title=D,yaxis_name=f"EMA{B[0]}",legend=I,watermark=J)
			for E in range(1,len(B)):M=A.ta.ema(length=B[E]);C=A.chart._add_line(line_chart=C,data_series=M,color=K[E],title=f"EMA - {B[E]} kỳ",yaxis_name=f"EMA{B[E]}")
		else:G=A.ta.ema(length=B).round(2);C=A.chart._line(time_series=L,data_series=G,color=F,title=D,yaxis_name='EMA',legend=I,watermark=J)
		if minimal:return A._minimal_chart(indicator_chart=C,title=D)
		else:return A._base_chart(indicator_chart=C,title=D)
	def vwap(A,anchor:str='D',title:str='VWAP - Volume Weighted Average Price',color=_ORANGE,legend=_A,watermark=_A,minimal:bool=_B):
		J=watermark;I=legend;H=color;D=title;B=anchor
		if H:F=H
		else:F=A.chart.mono_color
		if A.theme==_D:K=list(DARK_MODE_PRIMARY_COLORS.values())+list(DARK_MODE_SECONDARY_COLORS.values())
		else:K=list(LIGHT_MODE_PRIMARY_COLORS.values())+list(LIGHT_MODE_SECONDARY_COLORS.values())
		L=A.data.index.strftime(_C).tolist()
		if isinstance(B,list):
			G=A.ta.vwap(anchor=B[0]).round(2);C=A.chart._line(time_series=L,data_series=G,color=F,title=D,yaxis_name=f"VWAP - {B[0]}",legend=I,watermark=J)
			for E in range(1,len(B)):M=A.ta.vwap(anchor=B[E]);C=A.chart._add_line(line_chart=C,data_series=M,color=K[E],title=f"VWAP - {B[E]}",yaxis_name=f"VWAP - {B[E]}")
		else:G=A.ta.vwap(anchor=B);C=A.chart._line(time_series=L,data_series=G,color=F,title=D,yaxis_name='VWAP',legend=I,watermark=J)
		if minimal:return A._minimal_chart(indicator_chart=C,title=D)
		else:return A._base_chart(indicator_chart=C,title=D)
	def vwma(A,length:int=10,title:str='VWMA - Volume Weighted Moving Average',color=_ORANGE,legend=_A,watermark=_A,minimal:bool=_B):
		J=watermark;I=legend;H=color;D=title;B=length
		if H:F=H
		else:F=A.chart.mono_color
		if A.theme==_D:K=list(DARK_MODE_PRIMARY_COLORS.values())+list(DARK_MODE_SECONDARY_COLORS.values())
		else:K=list(LIGHT_MODE_PRIMARY_COLORS.values())+list(LIGHT_MODE_SECONDARY_COLORS.values())
		L=A.data.index.strftime(_C).tolist()
		if isinstance(B,list):
			G=A.ta.vwma(length=B[0]).round(2);C=A.chart._line(time_series=L,data_series=G,color=F,title=D,yaxis_name=f"VWMA{B[0]}",legend=I,watermark=J)
			for E in range(1,len(B)):M=A.ta.vwma(length=B[E]);C=A.chart._add_line(line_chart=C,data_series=M,color=K[E],title=f"VWMA - {B[E]}",yaxis_name=f"VWMA{B[E]}")
		else:G=A.ta.vwma(length=B);C=A.chart._line(time_series=L,data_series=G,color=F,title=D,yaxis_name='VWMA',legend=I,watermark=J)
		if minimal:return A._minimal_chart(indicator_chart=C,title=D)
		else:return A._base_chart(indicator_chart=C,title=D)
	def psar(A,af0:float=.02,af:float=.02,max_af:float=.2,title='Parabollic Stop and Reverse',color=_ORANGE,symbol_size=5,legend=_A,watermark=_A,minimal:bool=_B):
		C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		E=A.ta.psar(af0=af0,af=af,max_af=max_af).round(2);G=E.iloc[:,0].fillna(E.iloc[:,1]);H=A.data.index.strftime(_C).tolist();F=A.chart._scatter(time_series=H,data_series=G,color=D,symbol_size=symbol_size,title=B,yaxis_name='PSAR',legend=legend,watermark=watermark)
		if minimal:return A._minimal_chart(indicator_chart=F,title=B)
		else:return A._base_chart(indicator_chart=F,title=B)
	def supertrend(A,length:int=10,multiplier:float=3,title='Supertrend',color=[_ISLAND_GREEN,_ORANGE],legend=_A,watermark=_A,minimal:bool=_B):
		E=color;B=title
		if E:D=E
		else:D=A.chart.mono_color
		F=A.ta.supertrend(length=length,multiplier=multiplier).round(2);G=A.data.index.strftime(_C).tolist();C=A.chart._line(time_series=G,data_series=F.iloc[:,-2],color=D[0],title=B,yaxis_name='Up Trend',legend=legend,watermark=watermark);C=A.chart._add_line(line_chart=C,data_series=F.iloc[:,-1],color=D[1],title=B,yaxis_name='Down Trend')
		if minimal:return A._minimal_chart(indicator_chart=C,title=B)
		else:return A._base_chart(indicator_chart=C,title=B)
	def adx(A,length:int=14,title='Average Directional Index',color=_ORANGE,legend=_B,watermark=_A,minimal:bool=_B):
		C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		F=A.ta.adx(length=length).round(2).iloc[:,0];G=A.data.index.strftime(_C).tolist();H=opts.MarkLineOpts(data=[opts.MarkLineItem(y=25,name='Có xu hướng',linestyle_opts=opts.LineStyleOpts(width=1,color=_ISLAND_GREEN,opacity=.5,type_='dashed'))],label_opts=opts.LabelOpts(is_show=_B));E=A.chart._line(time_series=G,data_series=F,color=D,title=B,yaxis_name='ADX',legend=legend,watermark=watermark,show_xaxis=_B,markline_opts=H)
		if minimal:return A._minimal_chart(indicator_chart=E,title=B,layout=_E)
		else:return A._base_chart(indicator_chart=E,title=B,layout=_E)
	def aroon(A,length:int=14,title='Aroon',color=[_ISLAND_GREEN,_ORANGE],legend=_B,watermark=_A,minimal:bool=_B):
		E=color;B=title
		if E:D=E
		else:D=[_ISLAND_GREEN,_ORANGE]
		F=A.ta.aroon(length=length).round(2);G=F.iloc[:,1];H=F.iloc[:,0];I=A.data.index.strftime(_C).tolist();C=A.chart._line(time_series=I,data_series=G,color=D[0],title=B,yaxis_name='Aroon Up',is_smooth=_B,legend=legend,watermark=watermark,show_xaxis=_B);C=A.chart._add_line(line_chart=C,data_series=H,color=D[1],title=B,yaxis_name='Aroon Down',is_smooth=_B)
		if minimal:return A._minimal_chart(indicator_chart=C,title=B,layout=_E)
		else:return A._base_chart(indicator_chart=C,title=B,layout=_E)