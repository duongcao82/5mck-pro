# src/viz.py
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import timedelta
import numpy as np

# ==============================================================================
# BẢNG MÀU CHUẨN CSS & MAP RGB CHO ĐỘ MỜ (TRANSPARENCY)
# ==============================================================================
# Sử dụng tên màu CSS chuẩn của Plotly cho gọn gàng và tránh lỗi
C_1D_OB = 'darkred'      # Đỏ thẫm
C_1D_FVG = 'darkgreen'   # Xanh lá thẫm
C_1H_OB = 'orangered'    # Cam đỏ
C_1H_FVG = 'mediumblue'  # Xanh dương đậm vừa (chuẩn CSS)
C_15M_OB = 'gold'        # Vàng ánh kim
C_15M_FVG = 'cyan'       # Xanh lơ sáng
C_CONF_YELLOW = 'yellow' # Vàng tươi (cho hợp lưu)
C_NEUTRAL = 'gray'       # Màu trung tính mặc định

# Map từ tên màu CSS sang chuỗi RGB để dùng cho hàm rgba() khi cần làm mờ nền
_COLOR_TO_RGB_MAP = {
    'darkred': '139, 0, 0',
    'darkgreen': '0, 100, 0',
    'orangered': '255, 69, 0',
    'mediumblue': '0, 0, 205', 
    'gold': '255, 215, 0',
    'cyan': '0, 255, 255',
    'yellow': '255, 255, 0',
    'gray': '128, 128, 128',
    'white': '255, 255, 255'
}

# ==============================================================================
# 1. HÀM PHỤ TRỢ: VẼ ZONE (ĐÃ UPDATE LOGIC THEO YÊU CẦU MỚI)
# ==============================================================================
def _draw_zone_helper(fig, df, zones, interval, opacity=0.1, is_htf=False):
    """
    Vẽ các khối SMC với quy tắc màu sắc, viền và hợp lưu nghiêm ngặt theo từng khung thời gian.
    """
    if not zones: return
    if not isinstance(zones, list): zones = [zones]
    
    # --- SMART SCALE LOGIC ---
    curr_min = df['Low'].min()
    curr_max = df['High'].max()
    mid_price = (curr_min + curr_max) / 2
    threshold_filter = 500 if mid_price > 1000 else 0.05
    
    for z in zones:
        if z is None: continue
        y_low = z.get('y0', z.get('bottom'))
        y_high = z.get('y1', z.get('top'))
        if y_low is None or y_high is None: continue
        if y_low < threshold_filter or y_high < threshold_filter: continue 

        # --- BƯỚC 1: XÁC ĐỊNH THUỘC TÍNH ZONE (Giữ nguyên) ---
        zone_type_str = str(z.get('type', '')).lower()
        is_fvg = 'fvg' in zone_type_str
        is_bullish = z.get('side') == 'bull' or z.get('type') == 'demand' or 'BULL' in zone_type_str.upper()
        
        confluence_list = z.get('confluence', [])
        has_conf = len(confluence_list) > 0
        
        # Xác định nguồn gốc khung thời gian của zone
        zone_source_tf = '1H' if z.get('is_from_1h', False) else ('1D' if is_htf and interval != '1D' else interval)

        # Lọc zone xa (Giữ nguyên)
        if interval == '15m' and (y_high < curr_min * 0.97 or y_low > curr_max * 1.03): continue
        elif interval == '1H' and (y_high < curr_min * 0.95 or y_low > curr_max * 1.05): continue

        # ======================================================================
        # BẮT ĐẦU PHẦN SỬA ĐỔI LOGIC STYLE (BƯỚC 2 & 3 CŨ GỘP LẠI)
        # ======================================================================
        
        # ĐIỂM MẤU CHỐT: Chỉ tô viền vàng nếu có hợp lưu VÀ KHÔNG PHẢI là zone HTF đang làm nền
        should_highlight_yellow = has_conf and not is_htf

        # Khởi tạo mặc định
        base_color_name = C_NEUTRAL
        fill_opacity = 0.1
        final_line_color = C_NEUTRAL
        final_line_width = 0
        final_line_dash = 'solid'
        layer = "below"
        text_color = 'white' # Màu chữ mặc định

        # === LOGIC KHUNG 1D ===
        if interval == '1D':
            base_color_name = C_1D_FVG if is_bullish else C_1D_OB
            fill_opacity = 0.15
            # Rule: solid, width 0.3. Vàng nếu hợp lưu.
            final_line_width = 0.3
            final_line_dash = 'solid'
            if should_highlight_yellow:
                 final_line_color = C_CONF_YELLOW
                 text_color = C_CONF_YELLOW
            else:
                 final_line_color = base_color_name

        # === LOGIC KHUNG 1H ===
        elif interval == '1H':
             if is_htf or zone_source_tf == '1D': # Nền 1D trên chart 1H
                 base_color_name = C_1D_FVG if is_bullish else C_1D_OB
                 fill_opacity = 0.12
                 # Rule: dash, width 0.3, màu gốc (KHÔNG vàng)
                 final_line_color = base_color_name
                 final_line_width = 0.3
                 final_line_dash = 'dash'
             else: # Zone 1H chính chủ trên chart 1H
                 base_color_name = C_1H_FVG if is_fvg else C_1H_OB
                 fill_opacity = 0.25
                 # Rule: solid, width 0.5. Vàng nếu hợp lưu.
                 final_line_width = 0.5
                 final_line_dash = 'solid'
                 if should_highlight_yellow:
                     final_line_color = C_CONF_YELLOW
                     text_color = C_CONF_YELLOW
                 else:
                     final_line_color = base_color_name

        # === LOGIC KHUNG 15m ===
        elif interval == '15m':
            if is_htf: # Nền HTF trên chart 15m
                 layer = "below"
                 if zone_source_tf == '1D': # Nền 1D
                      base_color_name = C_1D_FVG if is_bullish else C_1D_OB
                      fill_opacity = 0.05
                      # Rule: dash, width 0.3, màu gốc 1D
                      final_line_color = base_color_name
                      final_line_width = 0.3
                      final_line_dash = 'dash'
                 else: # Nền 1H
                      base_color_name = C_1H_FVG if is_fvg else C_1H_OB
                      fill_opacity = 0.08
                      # Rule: dash, width 0.5, màu gốc 1H
                      final_line_color = base_color_name
                      final_line_width = 0.5
                      final_line_dash = 'dash'
            else: # Zone 15m chính chủ trên chart 15m
                 layer = "above" if not is_fvg else "below"
                 base_color_name = C_15M_FVG if is_fvg else C_15M_OB
                 fill_opacity = 0.35
                 # Rule: solid, width 1.0. Vàng nếu hợp lưu.
                 final_line_width = 1.0
                 final_line_dash = 'solid'
                 if should_highlight_yellow:
                     final_line_color = C_CONF_YELLOW
                     text_color = C_CONF_YELLOW
                 else:
                     final_line_color = base_color_name

        # --- TÍNH TOÁN MÀU NỀN CUỐI CÙNG ---
        # Nếu có hợp lưu vàng, tăng nhẹ độ đậm nền để nổi bật hơn (tối đa 50%)
        if should_highlight_yellow:
             fill_opacity = min(fill_opacity * 1.5, 0.5)

        # Tạo màu nền có độ mờ (cần dùng map RGB để tạo chuỗi rgba)
        base_rgb_values = _COLOR_TO_RGB_MAP.get(base_color_name, _COLOR_TO_RGB_MAP[C_NEUTRAL])
        final_fill_color_rgba = f"rgba({base_rgb_values}, {fill_opacity})"
        
        # ======================================================================
        # KẾT THÚC PHẦN SỬA ĐỔI LOGIC STYLE
        # ======================================================================

        # --- BƯỚC 4: VẼ SHAPE (Giữ nguyên cấu trúc cũ) ---
        fig.add_shape(
            type="rect",
            xref="x", yref="y",
            x0=z.get('start_idx', df.index[0]), 
            y0=y_low,
            x1=df.index[-1] + timedelta(days=5 if interval=='1D' else 1), 
            y1=y_high,
            fillcolor=final_fill_color_rgba, # Sử dụng màu nền đã tính toán
            layer=layer,
            # Sử dụng các thông số viền đã tính toán ở trên
            line=dict(width=final_line_width, color=final_line_color, dash=final_line_dash),
            row=1, col=1
        )

        # --- BƯỚC 5: GHI NHÃN (Cập nhật logic hiển thị nhãn) ---
        label = z.get('label', 'Zone') 
        if is_htf: label = f"HTF {label}"

        should_show_label = False
        # Chỉ hiện nhãn nếu có hợp lưu vàng HOẶC là OB chính chủ (không phải HTF)
        if should_highlight_yellow: should_show_label = True
        elif not is_htf and 'OB' in label.upper(): should_show_label = True

        if should_show_label:
            mid_y = (y_low + y_high) / 2
            font_weight = "Arial Black" if should_highlight_yellow else "Arial"
            
            fig.add_annotation(
                x=df.index[-1], y=mid_y, text=label, showarrow=False,
                # Truyền màu chữ đã xác định (Vàng hoặc Trắng)
                font=dict(size=10, color=text_color, family=font_weight),
                bgcolor="rgba(0,0,0,0.5)", xanchor="left", xshift=5, 
                row=1, col=1
            )

# ==============================================================================
# 2. HÀM VẼ CHÍNH (GIỮ NGUYÊN HOÀN TOÀN LOGIC CŨ)
# ==============================================================================
def plot_single_timeframe(
    df, symbol, interval, 
    smc_levels=None, fvg_zones=[], ob_zones=[], 
    htf_zones=[], trendlines=[], rsi_divergences=[], idm_point=None,
    smart_money_data=None,
    show_vol=True, show_ma=True, show_rsi=True, show_smc=True, show_vsa=False 
):
    if df is None or df.empty: return go.Figure()

    # --- SMART SCALE ---
    curr_mean = df['Close'].mean()
    threshold_filter = 500 if curr_mean > 1000 else 0.05

    # --- SETUP SUBPLOTS ---
    rows = 1; row_heights = [0.6] 
    vol_row = None; rsi_row = None

    if show_vol: rows += 1; vol_row = rows; row_heights.append(0.15)
    if show_rsi: rows += 1; rsi_row = rows; row_heights.append(0.25)
    
    flow_row = None
    if isinstance(smart_money_data, tuple):
        smart_money_data = smart_money_data[0]

    show_flow = smart_money_data is not None and isinstance(smart_money_data, pd.DataFrame) and not smart_money_data.empty
    
    if show_flow:
        rows += 1; flow_row = rows; row_heights.append(0.15)

    total_h = sum(row_heights)
    row_heights = [h/total_h for h in row_heights]

    fig = make_subplots(
        rows=rows, cols=1, shared_xaxes=True, vertical_spacing=0.02, row_heights=row_heights
    )

# --- A. PREMIUM / DISCOUNT & OTE (FIXED) ---
    lookback = min(len(df), 150) 
    if lookback > 50:
        recent = df.tail(lookback)
        h_val = float(recent['High'].max()); l_val = float(recent['Low'].min())
        
        if h_val > 0 and l_val > 0:
            range_val = h_val - l_val; eq_val = (h_val + l_val) / 2
            ote_62 = h_val - (range_val * 0.618); ote_78 = h_val - (range_val * 0.786)
            start_idx = recent.index[0]; end_idx = df.index[-1]

            # Premium (Đỏ nhạt)
            fig.add_shape(type="rect", xref="x", yref="y", x0=start_idx, x1=end_idx, y0=eq_val, y1=h_val,
                fillcolor="rgba(255, 82, 82, 0.04)", line_width=0, layer="below", row=1, col=1)
            # Discount (Xanh nhạt)
            fig.add_shape(type="rect", xref="x", yref="y", x0=start_idx, x1=end_idx, y0=l_val, y1=eq_val,
                fillcolor="rgba(0, 230, 118, 0.04)", line_width=0, layer="below", row=1, col=1)
            # OTE (Vàng)
            fig.add_shape(type="rect", xref="x", yref="y", x0=start_idx, x1=end_idx, y0=ote_78, y1=ote_62,
                fillcolor="rgba(255, 235, 59, 0.20)", line_width=0, layer="below", row=1, col=1)
            fig.add_annotation(x=start_idx, y=eq_val, text="EQ (0.5)", showarrow=False, 
                               font=dict(size=9, color="rgba(255,255,255,0.4)"), xanchor="left", row=1, col=1)

    # --- B. PRICE (CANDLESTICK) ---
    fig.add_trace(go.Candlestick(
        x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
        name='Price', increasing_line_color='#089981', decreasing_line_color='#F23645'
    ), row=1, col=1)

    # --- C. MOVING AVERAGES (MA) ---
    if show_ma:
        mas = [('SMA_10', '#2962ff', 0.8), ('SMA_20', '#ffd600', 0.8), ('EMA_50', '#ff3d00', 1.0), 
               ('EMA_100', '#00bcd4', 0.8), ('EMA_200', '#e0e0e0', 1.0)]
        for ma, color, width in mas:
            if ma in df.columns:
                fig.add_trace(go.Scatter(x=df.index, y=df[ma], line=dict(color=color, width=width), name=ma), row=1, col=1)

    # --- D. VSA SIGNALS ---
    if show_vsa and interval == '1D' and "VSA_Signal" in df.columns:
        vsa_df = df[df["VSA_Signal"] != "Normal"]
        if not vsa_df.empty:
            color_map = {"Buying Climax": "#ff9800", "Selling Climax": "#f44336", "No Demand": "#9c27b0", "No Supply": "#2196f3"}
            for sig, color in color_map.items():
                subset = vsa_df[vsa_df["VSA_Signal"] == sig]
                if not subset.empty:
                    fig.add_trace(go.Scatter(x=subset.index, y=subset['High']*1.02, mode='markers', marker=dict(size=6, color=color), name=f"VSA: {sig}"), row=1, col=1)

    # --- E. SMC ZONES (Sử dụng hàm helper mới) ---
    if show_smc:
        _draw_zone_helper(fig, df, ob_zones, interval, is_htf=False) 
        _draw_zone_helper(fig, df, fvg_zones, interval, is_htf=False) 
        if htf_zones:
            _draw_zone_helper(fig, df, htf_zones, interval, is_htf=True)

        if smc_levels:
            if 'Swing_High' in df.columns:
                sh = df[df['Swing_High'] & (df['High'] > threshold_filter)]
                fig.add_trace(go.Scatter(x=sh.index, y=sh['High'], mode='markers', marker=dict(symbol='triangle-down', size=5, color='yellow'), showlegend=False), row=1, col=1)
            if 'Swing_Low' in df.columns:
                sl = df[df['Swing_Low'] & (df['Low'] > threshold_filter)]
                fig.add_trace(go.Scatter(x=sl.index, y=sl['Low'], mode='markers', marker=dict(symbol='triangle-up', size=5, color='cyan'), showlegend=False), row=1, col=1)

    # --- F. TRENDLINES ---
    if trendlines:
        for tl in trendlines:
            if tl['y0'] > threshold_filter and tl['y1'] > threshold_filter:
                fig.add_shape(type="line", x0=tl['x0'], y0=tl['y0'], x1=tl['x1'], y1=tl['y1'], line=dict(color=tl['color'], width=2, dash="dash"), row=1, col=1)

    # --- G. SUB CHARTS (VOL & RSI) ---
    if show_vol and vol_row:
        colors = ['rgba(8, 153, 129, 0.5)' if c >= o else 'rgba(242, 54, 69, 0.5)' for c, o in zip(df['Close'], df['Open'])]
        fig.add_trace(go.Bar(x=df.index, y=df['Volume'], name='Volume', marker_color=colors), row=vol_row, col=1)
    
    if show_rsi and rsi_row and 'RSI_14' in df.columns:
        fig.add_trace(go.Scatter(x=df.index, y=df['RSI_14'], line=dict(color='#A64D79', width=1.2), name='RSI'), row=rsi_row, col=1)
        fig.add_hline(y=70, line_dash="dot", line_color="gray", row=rsi_row, col=1)
        fig.add_hline(y=30, line_dash="dot", line_color="gray", row=rsi_row, col=1)
        for div in rsi_divergences:
            c = '#00e676' if div['type'] == 'bull' else '#ff5252'
            fig.add_trace(go.Scatter(x=[df.index[div['i1']], df.index[div['i2']]], y=[df.iloc[div['i1']]['RSI_14'], df.iloc[div['i2']]['RSI_14']], mode='lines', line=dict(color=c, width=1.5), showlegend=False), row=rsi_row, col=1)

# --- H. ZOOM & CONFIG (GIỮ NGUYÊN LOGIC SCALE X, Y) ---
    if interval == '1D':
        zoom_count = 150   # 1D xem 100 nến
    elif interval == '1H':
        zoom_count = 60   # 1H xem 150 nến (Tăng số này để Zoom Out/nhìn xa hơn)
    elif interval == '15m':
        zoom_count = 70   # 15m xem 200 nến (Tăng số này để Zoom Out/nhìn xa hơn)
    else:
        zoom_count = 120   # Mặc định cho các khung khác
    buffer_candles = 60
    
    if len(df) > zoom_count:
        start_view = df.index[-zoom_count]
        last_date = df.index[-1]
        
        if interval == '1D':
            end_view = last_date + timedelta(days=buffer_candles * 1.5)
        elif interval == '1H':
            end_view = last_date + timedelta(hours=buffer_candles * 2.5)
        elif interval == '15m':
            end_view = last_date + timedelta(minutes=buffer_candles * 80)
        else:
            end_view = last_date + timedelta(days=20)

        fig.update_xaxes(range=[start_view, end_view], row=1, col=1)
        if vol_row: fig.update_xaxes(range=[start_view, end_view], row=vol_row, col=1)
        if rsi_row: fig.update_xaxes(range=[start_view, end_view], row=rsi_row, col=1)

    df_view = df.tail(zoom_count)
    visible_min = df_view['Low'].min()
    visible_max = df_view['High'].max()
    
    if not np.isnan(visible_min):
        p = 0.01 if interval in ['1H', '15m'] else 0.03
        fig.update_yaxes(range=[visible_min * (1 - p), visible_max * (1 + p)], row=1, col=1)

    rb_config = [dict(bounds=["sat", "mon"])]
    if interval in ['1H', '15m']:
        rb_config += [dict(pattern="hour", bounds=[15, 9]), dict(pattern="hour", bounds=[11.5, 13])]
    
    if show_flow and flow_row:
        net_col = [c for c in smart_money_data.columns if 'net' in c.lower()]
        if net_col:
            target = net_col[0]
            colors = ['#00e676' if v >= 0 else '#ff5252' for v in smart_money_data[target]]
            fig.add_trace(go.Bar(x=smart_money_data.index, y=smart_money_data[target], 
                                 name='Smart Money', marker_color=colors), row=flow_row, col=1)
    
    fig.update_xaxes(
        rangebreaks=rb_config, 
        showspikes=True, spikemode="across", spikesnap="cursor", 
        spikethickness=1, spikedash="dash", spikecolor="gray",
        row=1, col=1
    )
    
    fig.update_layout(
        template="plotly_dark", height=650 if show_rsi else 500,
        margin=dict(l=10, r=50, t=30, b=10), legend=dict(orientation="h", y=1.01, x=0),
        xaxis_rangeslider_visible=False, hovermode="x unified", dragmode="pan"
    )
    fig.update_yaxes(fixedrange=False, row=1, col=1)
    return fig

def plot_smart_money(df_foreign, df_prop, df_depth):
    figs = {}
    if not df_foreign.empty:
        df_f = df_foreign.tail(30)
        colors = ['#00e676' if v >= 0 else '#ff5252' for v in df_f['net_value']]
        fig_f = go.Figure(go.Bar(x=df_f.index, y=df_f['net_value'], marker_color=colors))
        fig_f.update_layout(title="🌍 Khối Ngoại (Tỷ VNĐ)", template="plotly_dark", height=250, margin=dict(l=10, r=10, t=40, b=10))
        figs['foreign'] = fig_f
    return figs