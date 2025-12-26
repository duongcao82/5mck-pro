_H='William %R'
_G='Quá bán'
_F='Quá mua'
_E='dashed'
_D='%Y-%m-%d'
_C=True
_B='subplot'
_A=False
import pandas as pd
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode
from vnstock_ta.utils.const import _ISLAND_GREEN,_ORANGE,_TURKISH_SEA,_SLATE_BLUE,_LIME_PUNCH,_GRADIENT_EMERALD,NEUTRAL_INFORMATION_COMPLETE,DARK_MODE_PRIMARY_COLORS,DARK_MODE_SECONDARY_COLORS,LIGHT_MODE_PRIMARY_COLORS,LIGHT_MODE_SECONDARY_COLORS
from vnstock_ta.chart.core import TAChart
class TAMomentum(TAChart):
	def __init__(A,data:pd.DataFrame,theme:str='light',watermark:bool=_C,display:bool=_C):super().__init__(data,theme,watermark,display)
	def rsi(A,length:int=14,title='Relative Strength Index',color=_ISLAND_GREEN,legend=_A,watermark=_C,minimal:bool=_A):
		C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		F=A.ta.rsi(length=length).round(2);G=A.data.index.strftime(_D).tolist();H=opts.MarkLineOpts(data=[opts.MarkLineItem(y=70,name=_F,linestyle_opts=opts.LineStyleOpts(width=1,color=_SLATE_BLUE,opacity=.5,type_=_E)),opts.MarkLineItem(y=30,name=_G,linestyle_opts=opts.LineStyleOpts(width=1,color=_SLATE_BLUE,opacity=.5,type_=_E))],label_opts=opts.LabelOpts(is_show=_A));E=A.chart._line(time_series=G,data_series=F,color=D,title=B,yaxis_name='RSI',is_smooth=_A,legend=legend,watermark=watermark,show_xaxis=_A,markline_opts=H)
		if minimal:return A._minimal_chart(indicator_chart=E,title=B,layout=_B)
		else:return A._base_chart(indicator_chart=E,title=B,layout=_B)
	def macd(A,fast:int=12,slow:int=26,signal:int=9,title='Moving Average Convergence Divergence',color=_ORANGE,legend=_A,watermark=_C,minimal:bool=_A):
		G=color;D=title
		if G:B=G
		else:B=A.chart.mono_color
		C=A.ta.macd(fast=fast,slow=slow,signal=signal).round(2);J=C.iloc[:,0];K=C.iloc[:,2];E=C.iloc[:,1]
		def L(histogram,prev_histogram):
			B=prev_histogram;A=histogram
			if A>0 and A>=B:return'#3CB371'
			elif A>0 and A<B:return'#90EE90'
			elif A<0 and A<=B:return'#FF6347'
			elif A<0 and A>B:return'#FFB6C1'
			else:return'#000000'
		M=[L(E.iloc[A],E.iloc[A-1]if A>0 else 0)for A in range(len(C))];B=JsCode('\n                            function(params) {\n                                var colors = %s;\n                                return colors[params.dataIndex];\n                            }\n                            '%M);H=A.data.index.strftime(_D).tolist();F=A.chart._line(time_series=H,data_series=J,color=B,title=D,yaxis_name='MACD',legend=legend,watermark=watermark,show_xaxis=_A);F=A.chart._add_line(line_chart=F,data_series=K,color=_ISLAND_GREEN,title='',yaxis_name='Signal');N=A.chart._hist(time_series=H,data_series=E,title='',color=B,yaxis_name='Histogram',show_xaxis=_A);I=N.overlap(F)
		if minimal:return A._minimal_chart(indicator_chart=I,title=D,layout=_B)
		else:return A._base_chart(indicator_chart=I,title=D,layout=_B)
	def willr(A,length:int=14,title=_H,color=_ISLAND_GREEN,legend=_A,watermark=_C,minimal:bool=_A):
		C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		F=A.ta.willr(length=length).round(2);G=A.data.index.strftime(_D).tolist();H=opts.MarkLineOpts(data=[opts.MarkLineItem(y=-20,name=_F,linestyle_opts=opts.LineStyleOpts(width=1,color=_SLATE_BLUE,opacity=.5,type_=_E)),opts.MarkLineItem(y=-80,name=_G,linestyle_opts=opts.LineStyleOpts(width=1,color=_SLATE_BLUE,opacity=.5,type_=_E))],label_opts=opts.LabelOpts(is_show=_A));E=A.chart._line(time_series=G,data_series=F,color=D,title=B,yaxis_name=_H,is_smooth=_A,legend=legend,watermark=watermark,show_xaxis=_A,markline_opts=H)
		if minimal:return A._minimal_chart(indicator_chart=E,title=B,layout=_B)
		else:return A._base_chart(indicator_chart=E,title=B,layout=_B)
	def cmo(A,length:int=14,title='Chande Momentum Oscilator',color=_ISLAND_GREEN,legend=_A,watermark=_C,minimal:bool=_A):
		C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		F=A.ta.cmo(length=length).round(2);G=A.data.index.strftime(_D).tolist();E=A.chart._line(time_series=G,data_series=F,color=D,title=B,yaxis_name='CMO',is_smooth=_A,legend=legend,watermark=watermark,show_xaxis=_A)
		if minimal:return A._minimal_chart(indicator_chart=E,title=B,layout=_B)
		else:return A._base_chart(indicator_chart=E,title=B,layout=_B)
	def stoch(A,k=14,d=3,smooth_k=3,title='Stochastic Oscillator',color=_ORANGE,legend=_A,watermark=_C,minimal:bool=_A):
		F='STOCH';D=color;C=title
		if D:G=D
		else:G=A.chart.mono_color
		E=A.ta.stoch(k=k,d=d,smooth_k=smooth_k);H=E.iloc[:,0];I=E.iloc[:,1];J=A.data.index.strftime(_D).tolist();K=opts.MarkLineOpts(data=[opts.MarkLineItem(y=80,name=_F,linestyle_opts=opts.LineStyleOpts(width=1,color=_SLATE_BLUE,opacity=.5,type_=_E)),opts.MarkLineItem(y=20,name=_G,linestyle_opts=opts.LineStyleOpts(width=1,color=_SLATE_BLUE,opacity=.5,type_=_E))],label_opts=opts.LabelOpts(is_show=_A));B=A.chart._line(time_series=J,data_series=H,color=_ISLAND_GREEN,title='',yaxis_name=F,is_smooth=_A,legend=legend,watermark=watermark,show_xaxis=_A,markline_opts=K);B=A.chart._add_line(line_chart=B,data_series=I,color=_ORANGE,title='',yaxis_name=F)
		if minimal:return A._minimal_chart(indicator_chart=B,title=C,layout=_B)
		else:return A._base_chart(indicator_chart=B,title=C,layout=_B)
	def roc(A,length:int=9,title='Rate of Change',color=_ISLAND_GREEN,legend=_A,watermark=_C,minimal:bool=_A):
		C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		F=A.ta.roc(length=length).round(2);G=A.data.index.strftime(_D).tolist();E=A.chart._line(time_series=G,data_series=F,color=D,title=B,yaxis_name='ROC',is_smooth=_A,legend=legend,watermark=watermark,show_xaxis=_A)
		if minimal:return A._minimal_chart(indicator_chart=E,title=B,layout=_B)
		else:return A._base_chart(indicator_chart=E,title=B,layout=_B)
	def mom(A,length:int=10,title='Momentum Indicator',color=_ISLAND_GREEN,legend=_A,watermark=_C,minimal:bool=_A):
		C=color;B=title
		if C:D=C
		else:D=A.chart.mono_color
		F=A.ta.mom(length=length).round(2);G=A.data.index.strftime(_D).tolist();E=A.chart._line(time_series=G,data_series=F,color=D,title=B,yaxis_name='MOM',is_smooth=_A,legend=legend,watermark=watermark,show_xaxis=_A)
		if minimal:return A._minimal_chart(indicator_chart=E,title=B,layout=_B)
		else:return A._base_chart(indicator_chart=E,title=B,layout=_B)