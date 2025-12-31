# src/viz.py
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import timedelta, datetime
import numpy as np

# ==============================================================================
# 1. CẤU HÌNH MÀU SẮC & GIAO DIỆN (THEME)
# ==============================================================================
# Bảng màu SMC chuẩn ICT
C_1D_OB = 'darkred'      # Đỏ thẫm (Kháng cự D1)
C_1D_FVG = 'darkgreen'   # Xanh lá thẫm (Hỗ trợ D1)
C_1H_OB = 'orangered'    # Cam đỏ (H1)
C_1H_FVG = 'mediumblue'  # Xanh dương đậm (H1)
C_15M_OB = 'gold'        # Vàng (15m)
C_15M_FVG = 'cyan'       # Xanh lơ (15m)
C_CONF_YELLOW = 'yellow' # Hợp lưu
C_NEUTRAL = 'gray'

# Map màu sang RGB để xử lý độ trong suốt (Opacity)
_COLOR_TO_RGB_MAP = {
    'darkred': '139, 0, 0', 'darkgreen': '0, 100, 0',
    'orangered': '255, 69, 0', 'mediumblue': '0, 0, 205',
    'gold': '255, 215, 0', 'cyan': '0, 255, 255',
    'yellow': '255, 255, 0', 'gray': '128, 128, 128', 'white': '255, 255, 255'
}

# ==============================================================================
# 2. HÀM PHỤ TRỢ: VẼ ZONE (CHI TIẾT)
# ==============================================================================
def _draw_zone_helper(fig, df, zones, interval, opacity=0.1, is_htf=False):
    """
    Hàm vẽ các khối SMC (OB, FVG) với logic hiển thị thông minh:
    - Phân loại màu theo khung thời gian (D1, H1, 15m).
    - Xử lý hợp lưu (Confluence) -> Tô viền vàng.
    - Ẩn bớt các zone quá xa giá hiện tại để đỡ rối mắt.
    """
    if not zones: return
    if not isinstance(zones, list): zones = [zones]
    
    # Tính toán ngưỡng lọc giá (để không vẽ rác)
    curr_min = df['Low'].min()
    curr_max = df['High'].max()
    mid_price = (curr_min + curr_max) / 2
    # Với CP > 1000đ thì lọc biên độ 500đ, ngược lại theo %
    threshold_filter = 50 if mid_price > 1000 else 0.05
    
    for z in zones:
        if z is None: continue
        y_low = z.get('y0', z.get('bottom'))
        y_high = z.get('y1', z.get('top'))
        
        # Bỏ qua zone lỗi hoặc quá bé
        if y_low is None or y_high is None: continue
        if abs(y_high - y_low) < threshold_filter * 0.1: continue 

        # --- LOGIC FILTER ZONE XA ---
        # 15m chỉ hiện zone quanh giá +- 3%
        if interval == '15m' and (y_high < curr_min * 0.97 or y_low > curr_max * 1.03): continue
        # 1H chỉ hiện zone quanh giá +- 5%
        elif interval == '1H' and (y_high < curr_min * 0.95 or y_low > curr_max * 1.05): continue

        # --- LOGIC STYLE ---
        zone_type_str = str(z.get('type', '')).lower()
        is_fvg = 'fvg' in zone_type_str
        # Xác định Phe: Bull (Cầu/Hỗ trợ) hay Bear (Cung/Kháng cự)
        is_bullish = z.get('side') == 'bull' or z.get('type') == 'demand' or 'BULL' in zone_type_str.upper()
        
        has_conf = len(z.get('confluence', [])) > 0
        
        # Xác định nguồn gốc zone (Zone này thuộc khung nào?)
        zone_source_tf = '1H' if z.get('is_from_1h', False) else ('1D' if is_htf and interval != '1D' else interval)

        # Mặc định
        should_highlight_yellow = has_conf and not is_htf
        base_color_name = C_NEUTRAL
        fill_opacity = 0.1
        final_line_color = C_NEUTRAL
        final_line_width = 0
        final_line_dash = 'solid'
        layer = "below"
        text_color = 'white'

        # --- CẤU HÌNH MÀU THEO KHUNG ---
        if interval == '1D':
            base_color_name = C_1D_FVG if is_bullish else C_1D_OB
            fill_opacity = 0.15
            final_line_width = 0.5
            if should_highlight_yellow:
                final_line_color = C_CONF_YELLOW
                text_color = C_CONF_YELLOW
            else:
                final_line_color = base_color_name

        elif interval == '1H':
             # Nếu là nền D1 hiển thị trên H1 -> Mờ hơn, nét đứt
             if is_htf or zone_source_tf == '1D': 
                 base_color_name = C_1D_FVG if is_bullish else C_1D_OB
                 fill_opacity = 0.1
                 final_line_width = 0.5
                 final_line_dash = 'dash'
                 final_line_color = base_color_name
             else:
                 base_color_name = C_1H_FVG if is_fvg else C_1H_OB
                 fill_opacity = 0.25
                 final_line_width = 0.8
                 if should_highlight_yellow:
                     final_line_color = C_CONF_YELLOW
                     text_color = C_CONF_YELLOW
                 else:
                     final_line_color = base_color_name

        elif interval == '15m':
            # Nếu là nền HTF (D1/H1) trên 15m -> Rất mờ, nằm dưới cùng
            if is_htf: 
                 layer = "below"
                 if zone_source_tf == '1D':
                      base_color_name = C_1D_FVG if is_bullish else C_1D_OB
                      fill_opacity = 0.05
                      final_line_width = 0.5
                      final_line_dash = 'dot'
                      final_line_color = base_color_name
                 else: # 1H
                      base_color_name = C_1H_FVG if is_fvg else C_1H_OB
                      fill_opacity = 0.08
                      final_line_width = 0.5
                      final_line_dash = 'dash'
                      final_line_color = base_color_name
            else:
                 # Zone 15m -> Đậm, nằm trên
                 layer = "above" if not is_fvg else "below"
                 base_color_name = C_15M_FVG if is_fvg else C_15M_OB
                 fill_opacity = 0.35
                 final_line_width = 1.0
                 if should_highlight_yellow:
                     final_line_color = C_CONF_YELLOW
                     text_color = C_CONF_YELLOW
                 else:
                     final_line_color = base_color_name

        # Tăng độ đậm nếu có Hợp lưu (Confluence)
        if should_highlight_yellow: 
            fill_opacity = min(fill_opacity * 1.5, 0.6)

        # Tạo chuỗi màu RGBA
        base_rgb = _COLOR_TO_RGB_MAP.get(base_color_name, _COLOR_TO_RGB_MAP[C_NEUTRAL])
        final_fill_color_rgba = f"rgba({base_rgb}, {fill_opacity})"
        
        # --- VẼ HÌNH CHỮ NHẬT ZONE ---
        fig.add_shape(
            type="rect", xref="x", yref="y",
            x0=z.get('start_idx', df.index[0]), 
            y0=y_low,
            x1=df.index[-1] + timedelta(days=5 if interval=='1D' else 1), 
            y1=y_high,
            fillcolor=final_fill_color_rgba, 
            layer=layer,
            line=dict(width=final_line_width, color=final_line_color, dash=final_line_dash),
            row=1, col=1
        )

        # --- VẼ NHÃN (LABEL) ---
        label = z.get('label', 'Zone') 
        if is_htf: label = f"HTF {label}"
        
        # Chỉ hiện nhãn cho Zone quan trọng hoặc có Hợp lưu để đỡ rối
        should_show_label = should_highlight_yellow or (not is_htf and 'OB' in label.upper())

        if should_show_label:
            fig.add_annotation(
                x=df.index[-1], y=(y_low + y_high)/2, 
                text=label, showarrow=False,
                font=dict(size=10, color=text_color, family="Arial Black" if should_highlight_yellow else "Arial"),
                bgcolor="rgba(0,0,0,0.6)", xanchor="left", xshift=5, 
                row=1, col=1
            )

# ==============================================================================
# 3. HÀM VẼ CHART CHÍNH (PLOT SINGLE TIMEFRAME)
# ==============================================================================
def plot_single_timeframe(
    df, symbol, interval, 
    smc_levels=None, fvg_zones=[], ob_zones=[], 
    htf_zones=[], trendlines=[], rsi_divergences=[], idm_point=None,
    smart_money_data=None,
    show_vol=True, show_ma=True, show_rsi=True, show_smc=True, show_vsa=False 
):
    """
    Hàm vẽ Chart chính đầy đủ tính năng: 
    Candles, MA, VSA, SMC Zones, OTE, Volume, RSI, Smart Money Flow.
    """
    if df is None or df.empty: return go.Figure()

    # --- CHUẨN BỊ SUBPLOTS ---
    # Row 1: Giá (60%)
    # Row 2: Volume (15%) - Nếu bật
    # Row 3: RSI (25%) - Nếu bật
    # Row 4: Smart Money Flow (15%) - Nếu có dữ liệu
    
    rows = 1
    row_heights = [0.6] 
    vol_row = None
    rsi_row = None
    flow_row = None

    if show_vol: 
        rows += 1
        vol_row = rows
        row_heights.append(0.15)
        
    if show_rsi: 
        rows += 1
        rsi_row = rows
        row_heights.append(0.20)
    
    # Xử lý dữ liệu Smart Money
    if isinstance(smart_money_data, tuple): 
        smart_money_data = smart_money_data[0] # Lấy df_foreign nếu trả về tuple
    
    show_flow = False
    if smart_money_data is not None and isinstance(smart_money_data, pd.DataFrame):
        if not smart_money_data.empty:
            show_flow = True
            rows += 1
            flow_row = rows
            row_heights.append(0.15)

    # Chuẩn hóa tỷ lệ chiều cao
    total_h = sum(row_heights)
    row_heights = [h/total_h for h in row_heights]

    fig = make_subplots(
        rows=rows, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.02, 
        row_heights=row_heights
    )

    # --------------------------------------------------------------------------
    # A. VẼ PREMIUM / DISCOUNT & OTE (ICT CONCEPTS)
    # --------------------------------------------------------------------------
    # Tính toán trên 150 nến gần nhất
    lookback = min(len(df), 150) 
    if lookback > 50:
        recent = df.tail(lookback)
        h_val = float(recent['High'].max())
        l_val = float(recent['Low'].min())
        
        if h_val > 0 and l_val > 0:
            range_val = h_val - l_val
            eq_val = (h_val + l_val) / 2
            ote_62 = h_val - (range_val * 0.618)
            ote_78 = h_val - (range_val * 0.786)
            
            start_idx = recent.index[0]
            end_idx = df.index[-1] + timedelta(days=5)

            # Vùng Premium (Đỏ nhạt - Bán)
            fig.add_shape(type="rect", xref="x", yref="y", 
                x0=start_idx, x1=end_idx, y0=eq_val, y1=h_val,
                fillcolor="rgba(255, 82, 82, 0.03)", line_width=0, layer="below", row=1, col=1)
            
            # Vùng Discount (Xanh nhạt - Mua)
            fig.add_shape(type="rect", xref="x", yref="y", 
                x0=start_idx, x1=end_idx, y0=l_val, y1=eq_val,
                fillcolor="rgba(0, 230, 118, 0.03)", line_width=0, layer="below", row=1, col=1)
            
            # Vùng OTE (Vàng - Entry Tối ưu)
            fig.add_shape(type="rect", xref="x", yref="y", 
                x0=start_idx, x1=end_idx, y0=ote_78, y1=ote_62,
                fillcolor="rgba(255, 235, 59, 0.15)", line_width=0, layer="below", row=1, col=1)
            
            # Nhãn EQ
            fig.add_annotation(x=start_idx, y=eq_val, text="EQ (0.5)", showarrow=False, 
                               font=dict(size=9, color="rgba(255,255,255,0.5)"), xanchor="left", row=1, col=1)

    # --------------------------------------------------------------------------
    # B. GIÁ (CANDLESTICK)
    # --------------------------------------------------------------------------
    fig.add_trace(go.Candlestick(
        x=df.index, 
        open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
        name='Price', 
        increasing_line_color='#089981', decreasing_line_color='#F23645'
    ), row=1, col=1)

    # --------------------------------------------------------------------------
    # C. MOVING AVERAGES (MA)
    # --------------------------------------------------------------------------
    if show_ma:
        # Danh sách MA cần vẽ: Tên cột, Màu, Độ dày
        mas = [
            ('SMA_10', '#2962ff', 0.8), 
            ('SMA_20', '#ffd600', 1.0), # Bollinger Middle
            ('EMA_50', '#ff3d00', 1.2), # Trend ngắn hạn
            ('EMA_200', '#e0e0e0', 1.5) # Trend dài hạn
        ] 
        for ma_col, color, width in mas:
            if ma_col in df.columns:
                fig.add_trace(go.Scatter(
                    x=df.index, y=df[ma_col], 
                    line=dict(color=color, width=width), 
                    name=ma_col
                ), row=1, col=1)

    # --------------------------------------------------------------------------
    # D. VSA SIGNALS (VOLUME SPREAD ANALYSIS)
    # --------------------------------------------------------------------------
    if show_vsa and interval == '1D' and "VSA_Signal" in df.columns:
        vsa_df = df[df["VSA_Signal"] != "Normal"]
        if not vsa_df.empty:
            color_map = {
                "Buying Climax": "#ff9800", # Cam
                "Selling Climax": "#f44336", # Đỏ
                "No Demand": "#9c27b0",      # Tím
                "No Supply": "#2196f3",      # Xanh dương
                "Test": "#00e676"            # Xanh lá
            }
            for sig, color in color_map.items():
                subset = vsa_df[vsa_df["VSA_Signal"] == sig]
                if not subset.empty:
                    fig.add_trace(go.Scatter(
                        x=subset.index, y=subset['High']*1.02, 
                        mode='markers', 
                        marker=dict(symbol='diamond', size=6, color=color), 
                        name=f"VSA: {sig}"
                    ), row=1, col=1)

    # --------------------------------------------------------------------------
    # E. SMC ZONES & SWING POINTS
    # --------------------------------------------------------------------------
    if show_smc:
        # Vẽ các Zone bằng hàm helper
        _draw_zone_helper(fig, df, ob_zones, interval, is_htf=False) 
        _draw_zone_helper(fig, df, fvg_zones, interval, is_htf=False) 
        if htf_zones:
            _draw_zone_helper(fig, df, htf_zones, interval, is_htf=True)

        # Vẽ Swing High / Swing Low (Đỉnh đáy quan trọng)
        if smc_levels:
            if 'Swing_High' in df.columns:
                sh = df[df['Swing_High']]
                if not sh.empty:
                    fig.add_trace(go.Scatter(
                        x=sh.index, y=sh['High'], 
                        mode='markers', marker=dict(symbol='triangle-down', size=5, color='yellow'), 
                        name='Swing High', showlegend=False
                    ), row=1, col=1)
            if 'Swing_Low' in df.columns:
                sl = df[df['Swing_Low']]
                if not sl.empty:
                    fig.add_trace(go.Scatter(
                        x=sl.index, y=sl['Low'], 
                        mode='markers', marker=dict(symbol='triangle-up', size=5, color='cyan'), 
                        name='Swing Low', showlegend=False
                    ), row=1, col=1)

    # --------------------------------------------------------------------------
    # F. TRENDLINES
    # --------------------------------------------------------------------------
    if trendlines:
        for tl in trendlines:
            # Lọc bớt trendline rác (giá trị 0)
            if tl.get('y0', 0) > 1 and tl.get('y1', 0) > 1:
                fig.add_shape(
                    type="line", 
                    x0=tl['x0'], y0=tl['y0'], 
                    x1=tl['x1'], y1=tl['y1'], 
                    line=dict(color=tl.get('color', 'yellow'), width=1.5, dash="dash"), 
                    row=1, col=1
                )

    # --------------------------------------------------------------------------
    # G. SUB CHARTS (VOLUME, RSI, FLOW)
    # --------------------------------------------------------------------------
    # 1. Volume
    if show_vol and vol_row:
        # Màu Volume theo nến tăng/giảm
        colors = ['rgba(8, 153, 129, 0.6)' if c >= o else 'rgba(242, 54, 69, 0.6)' for c, o in zip(df['Close'], df['Open'])]
        fig.add_trace(go.Bar(
            x=df.index, y=df['Volume'], 
            name='Volume', marker_color=colors
        ), row=vol_row, col=1)
    
    # 2. RSI
    if show_rsi and rsi_row and 'RSI_14' in df.columns:
        fig.add_trace(go.Scatter(
            x=df.index, y=df['RSI_14'], 
            line=dict(color='#A64D79', width=1.5), name='RSI'
        ), row=rsi_row, col=1)
        
        # Các đường 70 - 30
        fig.add_hline(y=70, line_dash="dot", line_color="gray", row=rsi_row, col=1)
        fig.add_hline(y=30, line_dash="dot", line_color="gray", row=rsi_row, col=1)
        fig.add_shape(type="rect", x0=df.index[0], x1=df.index[-1], y0=30, y1=70, 
                      fillcolor="rgba(128,128,128,0.1)", line_width=0, row=rsi_row, col=1)

        # Phân kỳ RSI
        for div in rsi_divergences:
            c = '#00e676' if div['type'] == 'bull' else '#ff5252'
            # Vẽ đoạn thẳng nối 2 đỉnh/đáy phân kỳ
            fig.add_trace(go.Scatter(
                x=[df.index[div['i1']], df.index[div['i2']]], 
                y=[df.iloc[div['i1']]['RSI_14'], df.iloc[div['i2']]['RSI_14']], 
                mode='lines+markers', 
                line=dict(color=c, width=2), 
                marker=dict(size=4),
                name='Div', showlegend=False
            ), row=rsi_row, col=1)

    # 3. Smart Money Flow
    if show_flow and flow_row:
        # Tìm cột Net Value (thường là net_value hoặc foreign_net_val)
        net_col = [c for c in smart_money_data.columns if 'net' in c.lower()]
        if net_col:
            target = net_col[0]
            # Căn chỉnh index cho khớp với df chính (quan trọng để không bị lệch trục)
            sm_aligned = smart_money_data.reindex(df.index).fillna(0)
            
            colors_sm = ['#00e676' if v >= 0 else '#ff5252' for v in sm_aligned[target]]
            fig.add_trace(go.Bar(
                x=sm_aligned.index, y=sm_aligned[target], 
                name='Smart Money Net', marker_color=colors_sm
            ), row=flow_row, col=1)

    # --------------------------------------------------------------------------
    # H. CẤU HÌNH ZOOM & HIỂN THỊ (QUAN TRỌNG)
    # --------------------------------------------------------------------------
    # Tính số lượng nến hiển thị mặc định (Zoom mặc định)
    zoom_count = 100 if interval == '1D' else (80 if interval == '1H' else 100)
    
    if len(df) > zoom_count:
        start_view = df.index[-zoom_count]
        last_date = df.index[-1]
        
        # Buffer thời gian tương lai để chart thoáng hơn
        if interval == '1D':
            end_view = last_date + timedelta(days=20)
        elif interval == '1H':
            end_view = last_date + timedelta(hours=30)
        else:
            end_view = last_date + timedelta(hours=10)

        # Áp dụng Range cho tất cả các trục X
        fig.update_xaxes(range=[start_view, end_view], row=1, col=1)
        if vol_row: fig.update_xaxes(range=[start_view, end_view], row=vol_row, col=1)
        if rsi_row: fig.update_xaxes(range=[start_view, end_view], row=rsi_row, col=1)
        if show_flow and flow_row: fig.update_xaxes(range=[start_view, end_view], row=flow_row, col=1)

    # Auto-scale trục Y dựa trên nến đang hiển thị
    df_view = df.tail(zoom_count)
    valid_lows = df_view['Low'][df_view['Low'] > 0]
    valid_highs = df_view['High'][df_view['High'] > 0]
    
    if not valid_lows.empty:
            visible_min = valid_lows.min()
            visible_max = valid_highs.max()
            
            # Padding trên dưới 2% (nhỏ hơn cũ để chart to hơn)
            p = 0.02 
            # Range Y cho chart giá
            fig.update_yaxes(range=[visible_min*(1-p), visible_max*(1+p)], row=1, col=1)

    # --------------------------------------------------------------------------
    # I. XỬ LÝ TRỤC THỜI GIAN (FIX LỖI RANGEBREAKS TOÀN CỤC)
    # --------------------------------------------------------------------------
    # Thay vì dùng biến toàn cục, ta định nghĩa trực tiếp rb_config ở đây
    rb_config = [
        dict(bounds=["sat", "mon"]), # Ẩn T7, CN
        # Có thể thêm list ngày lễ VN tại đây nếu muốn
        # dict(values=["2024-02-08", "2024-02-09"...]) 
    ]
    
    # Với khung giờ nhỏ, có thể ẩn khoảng trống qua đêm nếu muốn chart liền mạch
    if interval in ['1H', '15m']:
        # Ẩn từ 15h chiều đến 9h sáng hôm sau
        rb_config += [dict(pattern="hour", bounds=[15, 9])]
        # Ẩn nghỉ trưa 11h30 - 13h (tùy chọn)
        rb_config += [dict(pattern="hour", bounds=[11.5, 13])]
    
    # Cập nhật cấu hình trục X
    fig.update_xaxes(
        rangebreaks=rb_config, 
        showspikes=True, spikemode="across", spikesnap="cursor", 
        spikethickness=1, spikedash="dash", spikecolor="gray",
        row=1, col=1
    )
    # Update cho các chart phụ để đồng bộ
    if vol_row: fig.update_xaxes(rangebreaks=rb_config, row=vol_row, col=1)
    if rsi_row: fig.update_xaxes(rangebreaks=rb_config, row=rsi_row, col=1)
    if show_flow and flow_row: fig.update_xaxes(rangebreaks=rb_config, row=flow_row, col=1)
    
    # Layout cuối cùng
    fig.update_layout(
        template="plotly_dark", 
        height=700 if (show_rsi or show_flow) else 550,
        margin=dict(l=10, r=50, t=30, b=10), 
        legend=dict(orientation="h", y=1.01, x=0),
        xaxis_rangeslider_visible=False, 
        hovermode="x unified", 
        dragmode="pan"
    )
    
    # Cho phép trục Y scale tự do
    fig.update_yaxes(fixedrange=False, row=1, col=1)
    
    return fig

# ==============================================================================
# 4. HÀM VẼ SMART MONEY RIÊNG (TAB RIÊNG)
# ==============================================================================
def plot_smart_money(df_foreign, df_prop, df_depth):
    figs = {}
    
    # Chart Khối Ngoại
    if not df_foreign.empty:
        df_f = df_foreign.tail(30)
        colors = ['#00e676' if v >= 0 else '#ff5252' for v in df_f['net_value']]
        fig_f = go.Figure(go.Bar(
            x=df_f.index, y=df_f['net_value'], 
            marker_color=colors, name="Net Foreign"
        ))
        fig_f.update_layout(
            title="🌍 Khối Ngoại (Tỷ VNĐ)", template="plotly_dark", 
            height=300, margin=dict(l=10, r=10, t=40, b=10)
        )
        figs['foreign'] = fig_f
        
    # Chart Tự Doanh
    if not df_prop.empty:
        df_p = df_prop.tail(30)
        colors = ['#ffeb3b' if v >= 0 else '#ff5252' for v in df_p['net_value']]
        fig_p = go.Figure(go.Bar(
            x=df_p.index, y=df_p['net_value'], 
            marker_color=colors, name="Net Prop"
        ))
        fig_p.update_layout(
            title="🏢 Tự Doanh (Tỷ VNĐ)", template="plotly_dark", 
            height=300, margin=dict(l=10, r=10, t=40, b=10)
        )
        figs['prop'] = fig_p

    return figs