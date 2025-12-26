_u='Đồ thị kỹ thuật'
_t='volume'
_s='%Y-%m-%d'
_r='bottom'
_q='markline_opts'
_p='linear'
_o='global'
_n='colorStops'
_m='inside'
_l='Line Chart'
_k='Google Colab'
_j='open'
_i='center'
_h='title_opts'
_g='Bar Chart'
_f='type'
_e='colorful'
_d='negative'
_c='positive'
_b='LIGHT'
_a='overlap'
_Z='itemStyleColor'
_Y='close'
_X='offset'
_W='visualmap_opts'
_V='Candlestick Chart'
_U='minimal'
_T='Indicator'
_S='itemStyle'
_R='#fff'
_Q='900px'
_P='1500px'
_O='neutral'
_N='left'
_M='yaxis_opts'
_L='xaxis_opts'
_K='MainChart'
_J='right'
_I='Price'
_H='value'
_G='dark'
_F='DARK'
_E='Volume'
_D=None
_C='color'
_B=False
_A=True
import pandas as pd,numpy as np,panel as pn
from typing import List,Dict,Any,Union,Tuple
from pyecharts import options as opts
from pyecharts.charts import Kline,Line,Bar,Scatter,Boxplot,HeatMap,Grid,Page
from pyecharts.globals import ThemeType
from pyecharts.commons.utils import JsCode
from vnstock_ta.get_data import DataSource
from vnstock_ta.utils.env import SystemInfo
from vnstock_ta.utils.const import _EMERALD_GREEN,_CRIMSON_RED,_SLATE_BLUE,_GRADIENT_EMERALD
try:from IPython.display import display,HTML;IS_NOTEBOOK=_A
except ImportError:IS_NOTEBOOK=_B
class BaseChart:
	def __init__(A,candle_data:pd.DataFrame,theme:str=_G,color_category:str=_O,width:str=_P,height:str=_Q):B=color_category;A.theme=theme.upper();A.color_category=B;A.width=width;A.height=height;A.is_notebook=IS_NOTEBOOK;A.data=candle_data;A._validate_inputs();A._ui_config();A._config(theme=A.theme,color_category=B)
	def _validate_inputs(A):
		B=[_F,_b];C=[_c,_d,_O,_e]
		if A.theme not in B:raise ValueError(f"Unknown theme: {A.theme}. Valid themes are {B}")
		if A.color_category not in C:raise ValueError(f"Unknown category: {A.color_category}. Valid categories are {C}")
	def _ui_config(A):
		B=SystemInfo();A.interface=B.interface();A.hosting=B.hosting();A.os=B.os()
		if A.hosting==_k:pn.extension('echarts')
	def _render(A,chart:Any,display:bool=_A)->Union[pn.pane.ECharts]:
		C='chart.html';B=chart
		if display:
			if A.hosting==_k:return A._show_html(B.render(C))
			elif A.hosting=='Jupyterlab':return A._show_html(B.render(C))
			if A.interface=='Jupyter':return B.render_notebook()
			else:return B.render()
		else:return B
	def _show_html(A,filename:str):
		if A.is_notebook==_A:
			with open(filename,'r')as B:C=B.read()
			display(HTML(C))
	def _apply_color_palette(A,theme:str,color_category:str=_O):l='#006B38FF';k='#28334AFF';j='#606060FF';i='#A2A2A1FF';h='#949398FF';g='#F65058FF';f='#D7A9E3FF';e='#ADEFD1FF';d='#FEE715FF';c='#0063B2FF';b='#89ABE3FF';a='#EEA47FFF';Z='#F9F7FA';Y='#0E1114';X='#000000';W='#6D904F';V='#2C5F2D';U='#00539CFF';T='#2BAE66FF';S='#6A7793';R='#DA6A00';Q='#E5AE38';P='#37745B';O='#30A2DA';N='#FC4F30';M='indicator_5_color';L='indicator_4_color';K='indicator_3_color';J='indicator_2_color';I='indicator_1_color';H='indicator_color';G='text_color';F='mono_color';E='bg_color';D='bear_color';C='bull_color';m={_F:{_O:{C:_EMERALD_GREEN,D:_CRIMSON_RED,E:Y,F:_EMERALD_GREEN,G:_R,H:O,I:P,J:N,K:Q,L:R,M:S},_c:{C:T,D:a,E:Y,F:T,G:_R,H:b,I:U,J:c,K:d,L:e,M:f},_d:{C:V,D:g,E:'#101820FF',F:V,G:_R,H:U,I:h,J:i,K:j,L:k,M:l},_e:{C:W,D:N,E:Y,F:W,G:_R,H:O,I:P,J:N,K:Q,L:R,M:S}},_b:{_O:{C:_EMERALD_GREEN,D:_CRIMSON_RED,E:Z,F:_EMERALD_GREEN,G:X,H:O,I:P,J:N,K:Q,L:R,M:S},_c:{C:T,D:a,E:Z,F:T,G:X,H:b,I:U,J:c,K:d,L:e,M:f},_d:{C:V,D:g,E:'#FCF6F5FF',F:V,G:X,H:U,I:h,J:i,K:j,L:k,M:l},_e:{C:W,D:N,E:Z,F:W,G:X,H:O,I:P,J:N,K:Q,L:R,M:S}}};B=m[theme][color_category];A.bull_color=B[C];A.bear_color=B[D];A.bg_color=B[E];A.mono_color=B[F];A.text_color=B[G];A.indicator_color=B[H];A.indicator_1_color=B[I];A.indicator_2_color=B[J];A.indicator_3_color=B[K];A.indicator_4_color=B[L];A.indicator_5_color=B[M]
	def _common_global_opts(A,title:str,zoomable:bool,zoom_slider:bool,subplot:bool,legend:bool,tools:bool,watermark=_B)->Dict[str,Any]:
		H='legend_opts';G='lineX';F=title;E='all';D='title';C='show';B={'tooltip_opts':opts.TooltipOpts(trigger='axis',axis_pointer_type='cross',background_color=A.bg_color,border_width=1,border_color='#ccc',textstyle_opts=opts.TextStyleOpts(color=A.text_color)),'axispointer_opts':opts.AxisPointerOpts(is_show=_A,link=[{'xAxisIndex':E}],label=opts.LabelOpts(background_color='#777')),'graphic_opts':A._watermark(show=watermark)}
		if tools:B['toolbox_opts']=opts.ToolboxOpts(is_show=_A,orient='vertical',pos_left=_J,feature=opts.ToolBoxFeatureOpts(save_as_image={C:_A,D:'Save as Image','type_':'png','pixel_ratio':1},restore={C:_A,D:'Restore'},data_view={C:_A,D:'Data View'},data_zoom={C:_A,D:'Data Zoom'},magic_type={C:_A,D:'Magic Type',_f:['line','bar','stack','tiled'],'option':{'seriesIndex':E},'line_title':_l,'bar_title':_g,'stack_title':'Stack Chart','tiled_title':'Tiled Chart'}));B['brush_opts']=opts.BrushOpts(tool_box=['rect','polygon',G,'lineY','keep','clear'],x_axis_index=E,brush_link=E,out_of_brush={'colorAlpha':.1},brush_type=G)
		if zoomable:B['datazoom_opts']=[opts.DataZoomOpts(is_show=zoom_slider,type_='slider',xaxis_index=[0,1,2],range_start=0,range_end=100),opts.DataZoomOpts(is_show=_A,type_=_m,xaxis_index=[0,1,2],range_start=0,range_end=100)]
		if subplot:B[_h]=opts.TitleOpts(title=F,pos_left=_i,pos_top=20,title_textstyle_opts=opts.TextStyleOpts(font_family=A.font_family,font_size=A.font_size,color=A.text_color))
		else:B[_h]=opts.TitleOpts(title=F,pos_left=_i,pos_top=20,title_textstyle_opts=opts.TextStyleOpts(font_family=A.font_family,font_size=A.font_size*1.5,color=A.text_color))
		if legend:B[H]=opts.LegendOpts(is_show=_A)
		else:B[H]=opts.LegendOpts(is_show=_B)
		return B
	def _config(A,theme:str=_G,color_category:str=_O,width:str=_P,height:str=_Q):
		D=height;C=width;B=theme;A._apply_color_palette(B,color_category)
		if C:A.chart_width=C
		else:A.chart_width=_P
		if D:A.chart_height=D
		else:A.chart_height=_Q
		A.font_family='Nunito';A.font_size=18;A.logo_width=300;A.logo_height=112;A.logo_right_pos=int(A.chart_width.replace('px',''))/2-A.logo_width/2;A.logo_top_pos=int(A.chart_height.replace('px',''))/4;A.logo_opacity=.3;A.buy_marker='diamond';A.sell_marker='triangle';A.legend_top_pos=A.legend_bottom_pos=10;A.pos_left=_i;A.data_zoom_pos=_m;A.data_zoom_range_start=40;A.data_zoom_range_end=100;A.gridline_width=.3;A.gridline_opacity=.1;A.gridline_splitx=20;A.gridline_splity=2
		if B==_F:A.logo_path='https://vnstocks.com/img/vnstock_logo_trans_rec_hoz_bw.png'
		elif B==_b:A.logo_path='https://vnstocks.com/img/vnstock_logo_trans_rec_hoz.png'
	def _watermark(A,show:bool=_A)->List[opts.GraphicImage]:
		if show:B=[opts.GraphicImage(graphic_item=opts.GraphicItem(id_='logo',left=50,top=A.logo_top_pos,z=-10,bounding='raw',origin=[75,75]),graphic_imagestyle_opts=opts.GraphicImageStyleOpts(image=A.logo_path,width=A.logo_width,height=A.logo_height,opacity=A.logo_opacity))]
		else:B=[]
		return B
	def _volume(A,time_series:List[str],data_series:List[Union[Dict[str,Any],float]],title:str=_E,yaxis_name:str=_E,subplot=_A,compatibility:bool=_B,color:str=_SLATE_BLUE,label:bool=_B,zoomable:bool=_A,zoom_slider:bool=_B,legend=_B,theme:str=_G,watermark:bool=_B)->Bar:
		D=color;C=yaxis_name;B=data_series;G=ThemeType.DARK if theme.upper()==_F else ThemeType.LIGHT;H=opts.LabelOpts(is_show=_A)if compatibility else opts.LabelOpts(formatter=JsCode("\n                function(value) {\n                    if (value >= 1000000) {\n                        return (value / 1000000).toFixed(1) + 'M';\n                    } else if (value >= 1000) {\n                        return (value / 1000).toFixed(1) + 'K';\n                    }\n                    return value;\n                }\n            "))
		if not all(isinstance(A,dict)and _S in A for A in B):B=[{_H:A,_S:{_C:D}}for A in B]
		E=Bar(init_opts=opts.InitOpts(theme=G,bg_color=A.bg_color)).add_xaxis(xaxis_data=time_series).add_yaxis(series_name=C,y_axis=B,itemstyle_opts=opts.ItemStyleOpts(color=D),label_opts=opts.LabelOpts(is_show=label));I={_L:opts.AxisOpts(type_='category',is_scale=_A,boundary_gap=_B,axisline_opts=opts.AxisLineOpts(is_on_zero=_B),axistick_opts=opts.AxisTickOpts(is_show=_B),splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(opacity=A.gridline_opacity)),axislabel_opts=opts.LabelOpts(is_show=_A),split_number=A.gridline_splitx,min_='dataMin',max_='dataMax'),_M:opts.AxisOpts(name=C,is_scale=_A,split_number=A.gridline_splity,axislabel_opts=H,axisline_opts=opts.AxisLineOpts(is_show=_B),axistick_opts=opts.AxisTickOpts(is_show=_B),splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(opacity=A.gridline_opacity)),position=_J)};F=A._common_global_opts(title=title,zoomable=zoomable,zoom_slider=zoom_slider,subplot=subplot,legend=legend,tools=_B,watermark=watermark);F.pop(_h,_D);E.set_global_opts(**{**I,**F});return E
	def _kline(A,time_series:List[str],ohlc_data:List[List[float]],title:str=_V,yaxis_name:str=_I,right_y:bool=_A,show_xaxis:bool=_B,tools:bool=_A,watermark:bool=_B)->Kline:
		C=title
		if show_xaxis:D=opts.LabelOpts(is_show=_A);B=_A
		else:D=opts.LabelOpts(is_show=_B);B=_B
		F=_J if right_y else _N;E=Kline(init_opts=opts.InitOpts(theme=ThemeType.LIGHT,bg_color=A.bg_color)).add_xaxis(xaxis_data=time_series).add_yaxis(series_name=C,y_axis=ohlc_data,itemstyle_opts=opts.ItemStyleOpts(color=A.bull_color,color0=A.bear_color,border_color=A.bull_color,border_color0=A.bear_color));G={_M:opts.AxisOpts(name=yaxis_name,is_scale=_A,splitarea_opts=opts.SplitAreaOpts(is_show=_B,areastyle_opts=opts.AreaStyleOpts(opacity=1)),splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(opacity=A.gridline_opacity)),position=F),_L:opts.AxisOpts(is_show=B,is_inverse=B,axislabel_opts=D,splitarea_opts=opts.SplitAreaOpts(is_show=_B,areastyle_opts=opts.AreaStyleOpts(opacity=1)),splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(opacity=A.gridline_opacity))),_W:opts.VisualMapOpts(is_show=_B,dimension=2,series_index=5,is_piecewise=_A,pieces=[{_H:1,_C:A.bull_color},{_H:-1,_C:A.bear_color}])};H=A._common_global_opts(title=C,zoomable=_A,zoom_slider=_A,subplot=_B,legend=_B,tools=tools,watermark=watermark);E.set_global_opts(**{**G,**H});return E
	def _line(A,time_series:List[str],data_series:List[float],mark_points_data:List[opts.MarkPointItem]=_D,title:str=_l,yaxis_name:str=_I,right_y:bool=_A,color:str=_EMERALD_GREEN,is_step:bool=_B,is_smooth:bool=_A,area_style:bool=_B,gradient_color:Dict=_D,label:bool=_B,zoomable:bool=_A,subplot:bool=_B,legend:bool=_B,tools:bool=_A,show_xaxis:bool=_A,line_width:int=1,zoom_slider:bool=_B,theme:str=_G,watermark:bool=_B,**K)->Line:
		F=label;E=gradient_color;D=color;C=yaxis_name;L=_J if right_y else _N;M=ThemeType.DARK if theme.upper()==_F else ThemeType.LIGHT
		if E:G=E
		else:G={_f:_p,'x':0,'y':0,'x2':0,'y2':1,_n:[{_X:0,_C:_GRADIENT_EMERALD[0]},{_X:1,_C:_GRADIENT_EMERALD[1]}],_o:_B}
		if area_style:H=opts.AreaStyleOpts(opacity=.2,color=G)
		else:H=_D
		if show_xaxis:I=opts.LabelOpts(is_show=_A)
		else:I=opts.LabelOpts(is_show=_B)
		B=K.get(_q,_D)
		if B:B=B
		else:B=_D
		J=Line(init_opts=opts.InitOpts(theme=M,bg_color=A.bg_color)).add_xaxis(time_series).add_yaxis(series_name=C,y_axis=data_series,is_step=is_step,is_smooth=is_smooth,is_symbol_show=F,areastyle_opts=H,linestyle_opts=opts.LineStyleOpts(width=line_width,color=D),itemstyle_opts=opts.ItemStyleOpts(color=D),markpoint_opts=opts.MarkPointOpts(data=mark_points_data,label_opts=opts.LabelOpts(position=_r,color=_R)),markline_opts=B,label_opts=opts.LabelOpts(is_show=F));N={_L:opts.AxisOpts(splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity)),axislabel_opts=I),_M:opts.AxisOpts(name=C,splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity)),position=L)};O=A._common_global_opts(title=title,zoomable=zoomable,zoom_slider=zoom_slider,subplot=subplot,legend=legend,tools=tools,watermark=watermark);J.set_global_opts(**{**N,**O});return J
	def _base_line(F,time_series:List[str],data_series:List[float],title:str='Base Line',yaxis_name:str=_I,right_y:bool=_A,up_color:str=_EMERALD_GREEN,down_color:str=_CRIMSON_RED,base_value:int=1,values_range:List[int]=_D,dimension:int=1,show_visualmap:bool=_A,tools:bool=_A,is_step:bool=_B,area_style:bool=_B,label:bool=_B,zoomable:bool=_A,subplot:bool=_B,legend:bool=_B,show_xaxis:bool=_A,zoom_slider:bool=_B,theme:str=_G,watermark:bool=_B,**N)->Line:
		L=title;H=down_color;G=up_color;E='gt';D='lte';B=base_value;A=values_range;O=ThemeType.DARK if theme.upper()==_F else ThemeType.LIGHT;P=_J if right_y else _N
		if dimension not in[0,1]:raise ValueError('Dimension must be 0 or 1')
		if show_xaxis:M=opts.LabelOpts(is_show=_A)
		else:M=opts.LabelOpts(is_show=_B)
		I=N.get(_q,_D)
		if I:I=I
		else:I=_D
		J=Line(init_opts=opts.InitOpts(theme=O,bg_color=F.bg_color)).add_xaxis(xaxis_data=time_series);J.add_yaxis(series_name=L,y_axis=data_series,is_step=is_step,areastyle_opts=opts.AreaStyleOpts(opacity=.2,color=G)if area_style else _D,is_smooth=_A,is_symbol_show=label,linestyle_opts=opts.LineStyleOpts(width=1),markline_opts=I);C=[]
		if A is not _D:
			A=sorted(A)
			if B<A[0]:C.append({D:B,_C:H});C.append({E:B,D:A[0],_C:G})
			else:C.append({D:A[0],_C:H});C.append({E:A[0],D:B,_C:H})
			for K in range(1,len(A)):C.append({E:A[K-1],D:A[K],_C:G if A[K]>B else H})
			if A[-1]<B:C.append({E:A[-1],D:B,_C:H});C.append({E:B,_C:G})
			else:C.append({E:A[-1],_C:G})
		else:C=[{D:B,_C:H},{E:B,_C:G}]
		Q={_L:opts.AxisOpts(splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=F.gridline_width,opacity=F.gridline_opacity)),axislabel_opts=M),_M:opts.AxisOpts(name=yaxis_name,splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=F.gridline_width,opacity=F.gridline_opacity)),position=P),_W:opts.VisualMapOpts(is_show=show_visualmap,is_piecewise=_A,dimension=1,pieces=C)};R=F._common_global_opts(title=L,zoomable=zoomable,zoom_slider=zoom_slider,subplot=subplot,legend=legend,tools=tools,watermark=watermark);J.set_global_opts(**{**Q,**R});return J
	def _add_line(F,line_chart:Line,data_series:List[float],title:str='Additional Line',yaxis_name='Line',color:str=_EMERALD_GREEN,is_step:bool=_B,is_smooth:bool=_A,area_style:bool=_B,gradient_color:Dict=_D,label:bool=_B)->Line:
		C=gradient_color;B=color;A=line_chart
		if C:D=C
		else:D={_f:_p,'x':0,'y':0,'x2':0,'y2':1,_n:[{_X:0,_C:_GRADIENT_EMERALD[0]},{_X:1,_C:_GRADIENT_EMERALD[1]}],_o:_B}
		if area_style:E=opts.AreaStyleOpts(opacity=.2,color=D)
		else:E=_D
		A.add_yaxis(series_name=yaxis_name,y_axis=data_series,is_step=is_step,areastyle_opts=E,is_smooth=is_smooth,is_symbol_show=label,linestyle_opts=opts.LineStyleOpts(color=B),itemstyle_opts=opts.ItemStyleOpts(color=B));return A
	def _multi_lines(A,time_series:List[str],series_list:List[List[float]],color_list:List[str],title_list:List[str]=[_T],yaxis_name:str=_I,right_y:bool=_B,mark_points_data:List[opts.MarkPointItem]=_D,zoomable:bool=_A,subplot:bool=_B,legend:bool=_B,watermark:bool=_B)->Line:
		E=title_list;D=color_list;C=series_list
		if len(C)!=len(D):raise ValueError('series_list and color_list must have the same length')
		F=_J if right_y else _N;B=Line(init_opts=opts.InitOpts(theme=ThemeType.LIGHT)).add_xaxis(xaxis_data=time_series)
		for(G,H,I)in zip(E,D,C):B.add_yaxis(series_name=G,y_axis=I,is_smooth=_A,linestyle_opts=opts.LineStyleOpts(width=1,color=H),is_symbol_show=_B,markpoint_opts=opts.MarkPointOpts(data=mark_points_data,label_opts=opts.LabelOpts(position=_r,color=_R)))
		J={_L:opts.AxisOpts(splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity))),_M:opts.AxisOpts(name=yaxis_name,splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity)),position=F)};K=A._common_global_opts(title=E[0],zoomable=zoomable,zoom_slider=_A,subplot=subplot,legend=legend,tools=_B,watermark=watermark);B.set_global_opts(**{**J,**K});return B
	def _bar(A,time_series:List[str],data_series:List[Union[Dict[str,Any],float]],title:str=_g,yaxis_name:str=_I,right_y:bool=_A,show_xaxis:bool=_B,color:str=_EMERALD_GREEN,label:bool=_B,zoomable:bool=_A,zoom_slider:bool=_B,subplot:bool=_B,legend:bool=_B,tools:bool=_A,theme:str=_G,watermark:bool=_B)->Bar:
		D=color;C=title;B=data_series;G=ThemeType.DARK if theme.upper()==_F else ThemeType.LIGHT;H=_J if right_y else _N
		if not all(isinstance(A,dict)and _S in A for A in B):B=[{_H:A,_S:{_C:D}}for A in B]
		if show_xaxis:E=opts.LabelOpts(is_show=_A)
		else:E=opts.LabelOpts(is_show=_B)
		F=Bar(init_opts=opts.InitOpts(theme=G,bg_color=A.bg_color)).add_xaxis(xaxis_data=time_series).add_yaxis(series_name=C,y_axis=B,itemstyle_opts=opts.ItemStyleOpts(color=D),label_opts=opts.LabelOpts(is_show=label));I={_L:opts.AxisOpts(splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity))),_M:opts.AxisOpts(name=yaxis_name,splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity)),position=H,axislabel_opts=E),_W:opts.VisualMapOpts(is_show=_B,dimension=2,series_index=5,is_piecewise=_A,pieces=[{_H:1,_C:A.bull_color},{_H:-1,_C:A.bear_color}])};J=A._common_global_opts(title=C,zoomable=zoomable,zoom_slider=zoom_slider,subplot=_B,legend=legend,tools=tools,watermark=watermark);F.set_global_opts(**{**I,**J});return F
	def _hist(A,time_series:List[str],data_series:pd.Series,title:str=_g,yaxis_name:str=_I,right_y:bool=_A,show_xaxis:bool=_B,color:str=_EMERALD_GREEN,label:bool=_B,zoomable:bool=_A,zoom_slider:bool=_B,subplot:bool=_B,legend:bool=_B,tools:bool=_A,theme:str=_G,watermark:bool=_B)->Bar:
		B=title;E=ThemeType.DARK if theme.upper()==_F else ThemeType.LIGHT;F=_J if right_y else _N
		if show_xaxis:C=opts.LabelOpts(is_show=_A)
		else:C=opts.LabelOpts(is_show=_B)
		D=Bar(init_opts=opts.InitOpts(theme=E,bg_color=A.bg_color)).add_xaxis(xaxis_data=time_series).add_yaxis(series_name=B,y_axis=data_series.to_list(),itemstyle_opts=opts.ItemStyleOpts(color=color),label_opts=opts.LabelOpts(is_show=label));G={_L:opts.AxisOpts(splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity))),_M:opts.AxisOpts(name=yaxis_name,splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity)),position=F,axislabel_opts=C),_W:opts.VisualMapOpts(is_show=_B,dimension=2,series_index=5,is_piecewise=_A,pieces=[{_H:1,_C:A.bull_color},{_H:-1,_C:A.bear_color}])};H=A._common_global_opts(title=B,zoomable=zoomable,zoom_slider=zoom_slider,subplot=_B,legend=legend,tools=tools,watermark=watermark);D.set_global_opts(**{**G,**H});return D
	def _scatter(A,time_series:List[str],data_series:List[List[float]],title:str='Scatter Chart',yaxis_name:str=_I,right_y:bool=_A,color:str=_EMERALD_GREEN,symbol_size:int=5,label:bool=_B,zoomable:bool=_A,zoom_slider:bool=_B,subplot:bool=_B,legend:bool=_B,tools:bool=_A,theme:str=_G,watermark:bool=_B)->Scatter:C=title;D=ThemeType.DARK if theme.upper()==_F else ThemeType.LIGHT;E=_J if right_y else _N;B=Scatter(init_opts=opts.InitOpts(theme=D,bg_color=A.bg_color)).add_xaxis(xaxis_data=time_series);B.add_yaxis(series_name=C,y_axis=data_series,symbol_size=symbol_size,label_opts=opts.LabelOpts(is_show=label),itemstyle_opts=opts.ItemStyleOpts(color=color));F={_L:opts.AxisOpts(splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity))),_M:opts.AxisOpts(name=yaxis_name,splitline_opts=opts.SplitLineOpts(is_show=_A,linestyle_opts=opts.LineStyleOpts(width=A.gridline_width,opacity=A.gridline_opacity)),position=E)};G=A._common_global_opts(C,zoomable,zoom_slider,subplot,legend,tools);B.set_global_opts(**{**F,**G});return B
	def _candlestick(A,title:str=_V,yaxis_name:bool=_I,tools:bool=_A,watermark:bool=_A,display:bool=_B)->Grid:
		B=title;C=A.data.index.strftime(_s).to_list();F=A.data[[_j,_Y,'low','high']].values.tolist();A.data[_Z]=np.where(A.data[_j]>A.data[_Y],A.bear_color,A.bull_color);G=[{_H:A[_t],_S:{_C:A[_Z]}}for(B,A)in A.data.iterrows()];D=A._kline(time_series=C,ohlc_data=F,title=B,yaxis_name=yaxis_name,right_y=_A,tools=tools,watermark=watermark,show_xaxis=_B);E=A._volume(time_series=C,data_series=G,title=B,yaxis_name=_E)
		if display:A._grid_layout(chart_series={_K:D,_E:E},layout=_U)
		else:return D,E
	def _price_volume(A,title:str='Area Price and Volume Chart',yaxis_name:bool=_I,area_style:bool=_A,tools:bool=_A,watermark:bool=_A,display=_B)->Grid:
		B=title;C=A.data.index.strftime(_s).to_list();F=A.data[_Y].values.tolist();A.data[_Z]=np.where(A.data[_j]>A.data[_Y],A.bear_color,A.bull_color);G=[{_H:A[_t],_S:{_C:A[_Z]}}for(B,A)in A.data.iterrows()];D=A._line(time_series=C,data_series=F,title=B,yaxis_name=yaxis_name,right_y=_A,color=A.bull_color,area_style=area_style,zoomable=_A,tools=tools,watermark=watermark,show_xaxis=_B);E=A._volume(time_series=C,data_series=G,title=B,yaxis_name=_E)
		if display:return A._grid_layout(chart_series={_K:D,_E:E},layout=_U)
		else:return D,E
	def _grid_layout(L,chart_series:Dict[str,Any],layout:str=_a,chart_height:str=_Q,chart_width:str=_P,theme:str=_G,bg_color:str=_D)->Grid:
		J='77%';I='65%';H='13%';G=bg_color;F=layout;E=theme;D='7%';C='3%';A=chart_series;E=E.upper();K=ThemeType.DARK if E==_F else ThemeType.LIGHT
		if G is _D:G='#0E1117'if E==_F else'#FFFFFF'
		B=Grid(init_opts=opts.InitOpts(width=chart_width,height=chart_height,animation_opts=opts.AnimationOpts(animation=_A),theme=K,bg_color=G))
		if F==_U:B.add(A[_K],grid_opts=opts.GridOpts(pos_left=C,pos_right=D,height=I));B.add(A[_E],grid_opts=opts.GridOpts(pos_left=C,pos_right=D,pos_top=J,height=H))
		elif F==_a:B.add(A[_K].overlap(A[_T]),grid_opts=opts.GridOpts(pos_left=C,pos_right=D,height=I));B.add(A[_E],grid_opts=opts.GridOpts(pos_left=C,pos_right=D,pos_top=J,height=H))
		elif F=='subplot':B.add(A[_K],grid_opts=opts.GridOpts(pos_left=C,pos_right=D,height='50%'));B.add(A[_E],grid_opts=opts.GridOpts(pos_left=C,pos_right=D,pos_top='62%',height='11%'));B.add(A[_T],grid_opts=opts.GridOpts(pos_left=C,pos_right=D,pos_top='80%',height=H))
		return B
	def base_trading_chart(A,title:str=_V,display:bool=_A,tools:bool=_A,watermark:bool=_A,chart_width=_P,chart_height=_Q)->Union[pn.pane.ECharts,str]:
		C=chart_height;B=chart_width
		if B:D=B
		else:D=A.chart_width
		if C:E=C
		else:E=A.chart_height
		F,G=A._candlestick(title=title,yaxis_name=_I,watermark=watermark,tools=tools,display=_B);H=A._grid_layout(chart_series={_K:F,_E:G},layout=_U,chart_height=E,chart_width=D,theme=A.theme,bg_color=A.bg_color);return A._render(chart=H,display=display)
	def minimal_trading_chart(A,title:str=_V,area_style:bool=_B,tools:bool=_A,watermark:bool=_A,display:bool=_A,chart_width=_P,chart_height=_Q)->Union[pn.pane.ECharts,str]:
		C=chart_height;B=chart_width
		if B:D=B
		else:D=A.chart_width
		if C:E=C
		else:E=A.chart_height
		F,G=A._price_volume(title=title,display=_B,area_style=area_style,tools=tools,watermark=watermark);H=A._grid_layout(chart_series={_K:F,_E:G},layout=_U,chart_height=A.chart_height,chart_width=A.chart_width,theme=A.theme,bg_color=A.bg_color);return A._render(chart=H,display=display)
class TAChart:
	def __init__(A,data,theme:str=_G,watermark:bool=_B,display:bool=_A):A.data=data;A.theme=theme;A.watermark=watermark;A.display=display;A.ta=A._import_indicator()(data=A.data);A.chart=BaseChart(candle_data=A.data,theme=A.theme)
	def _import_indicator(B):from vnstock_ta.interface import Indicator as A;return A
	def _base_chart(A,indicator_chart,title=_u,width:str=_D,height:str=_D,layout:str=_a,watermark:bool=_D):
		D=watermark;C=height;B=width
		if D:E=D
		else:E=A.watermark
		if B:F=B
		else:F=A.chart.chart_width
		if C:G=C
		else:G=A.chart.chart_height
		H,I=A.chart._candlestick(title=title,watermark=E);J=A.chart._grid_layout(chart_series={_K:H,_E:I,_T:indicator_chart},layout=layout,chart_height=G,chart_width=F,theme=ThemeType.WHITE,bg_color=A.chart.bg_color);return A.chart._render(J,display=A.display)
	def _minimal_chart(A,indicator_chart,title=_u,width:str=_D,height:str=_D,layout:str=_a,watermark:bool=_D):
		D=watermark;C=height;B=width
		if D:E=D
		else:E=A.watermark
		if B:F=B
		else:F=A.chart.chart_width
		if C:G=C
		else:G=A.chart.chart_height
		H,I=A.chart._price_volume(title=title,area_style=_B,watermark=E);J=A.chart._grid_layout(chart_series={_K:H,_E:I,_T:indicator_chart},layout=layout,chart_height=G,chart_width=F,theme=ThemeType.WHITE,bg_color=A.chart.bg_color);return A.chart._render(J,display=A.display)