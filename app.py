import os
import sys
import time
import streamlit as st

# ==============================================================================
# 1. SETUP CƠ BẢN & UI SKELETON (CHẠY NGAY LẬP TỨC)
# ==============================================================================
# BẮT BUỘC: Lệnh này phải ở dòng đầu tiên
st.set_page_config(page_title="5MCK Pro", layout="wide", page_icon="📈")

# [FIX] Cấu hình Matplotlib Backend để tránh lỗi Thread trên Cloud
try:
    import matplotlib
    matplotlib.use('Agg')
except ImportError:
    pass

# Fix path để import các module trong thư mục src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# --- A. VẼ GIAO DIỆN CƠ BẢN TRƯỚC (Để qua mặt Healthcheck 503) ---
# CSS tùy chỉnh
st.markdown(
    """
    <style>
        .stApp { background-color: #0e1117; color: white; } 
        .metric-card { background-color: #262730; padding: 10px; border-radius: 5px; margin-bottom: 10px; }
        /* Ẩn bớt padding mặc định của Streamlit */
        .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Init Session State cơ bản
if "current_symbol" not in st.session_state:
    st.session_state.current_symbol = "VNINDEX"

# Định nghĩa Default String tạm thời (để hiện UI ngay mà không cần load universe.py)
# Khi load xong modules, biến này sẽ được update từ file universe.py
if "scan_symbols_text" not in st.session_state:
    st.session_state.scan_symbols_text = "ACB, HPG, SSI, VND, VCB, BID, CTG, VHM, VIC, VRE, FPT, MWG, MSN, GVR, GAS, POW, PLX, STB, TCB, TPB, MBB, VIB, VPB, HDB, OCB, SHB, LPB, MSB, SSB, EIB"

# --- B. VẼ SIDEBAR NGAY LẬP TỨC (Để User thấy App đang sống) ---
st.sidebar.empty()
current_dir = os.path.dirname(os.path.abspath(__file__))

# 1. LOGO
img_vps = os.path.join(current_dir, "5MCK_VPS.jpg")
if os.path.exists(img_vps):
    try: st.sidebar.image(img_vps, width=None) 
    except: st.sidebar.title("🎛️ 5MCK Pro")
else:
    st.sidebar.title("🎛️ 5MCK Pro")

# 2. INPUT MÃ & NÚT BC (Vẽ giao diện trước, xử lý logic sau)
c_search, c_btn = st.sidebar.columns([2, 1])
with c_search:
    symbol_input = st.text_input("🔍 Mã:", value=st.session_state.current_symbol, label_visibility="collapsed").upper()
with c_btn:
    btn_vnindex = st.button("📢", help="Báo cáo VNINDEX") 

# Logic đổi mã nhanh
if symbol_input != st.session_state.current_symbol:
    st.session_state.current_symbol = symbol_input
    st.rerun()

st.sidebar.markdown("---")

# 3. CẤU HÌNH CHART
st.sidebar.caption("⚙️ Cấu hình hiển thị")
col_cfg1, col_cfg2 = st.sidebar.columns(2)

with col_cfg1:
    use_ma = st.checkbox("MAs", value=False)
    use_rsi = st.checkbox("RSI", value=True)
    use_vol = st.checkbox("Vol", value=True)
    use_smart_money = st.checkbox("S.Money", value=False)

with col_cfg2:
    use_vsa = st.checkbox("VSA", value=False)
    use_smc = st.checkbox("SMC", value=True)
    use_trendline = st.checkbox("Trend", value=True)

# 4. MONEY MANAGEMENT
with st.sidebar.expander("💰 Quản lý vốn (NAV)", expanded=False):
    input_nav = st.number_input("Vốn (NAV)", value=1_000_000_000, step=100_000_000)
    input_risk = st.slider("Risk %", 0.5, 5.0, 1.0) / 100
    input_max_pos = st.number_input("Max Pos", value=4)


# ==============================================================================
# 2. HEAVY LOADING (LAZY LOADING - CHỈ LOAD KHI CẦN)
# ==============================================================================

@st.cache_resource(show_spinner="Đang khởi động hệ thống phân tích...")
def init_modules():
    """
    Hàm này chứa tất cả các import nặng.
    Nó chỉ chạy 1 lần duy nhất khi khởi động App.
    """
    import concurrent.futures
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import pandas as pd
    import plotly.graph_objects as go

    # Config Pandas
    pd.set_option("future.no_silent_downcasting", True)

    # --- IMPORT MODULES NỘI BỘ ---
    # Import ở đây để không chặn UI loading ban đầu
    from universe import get_vnallshare_universe, RAW_TICKERS_STR # Lấy thêm RAW_TICKERS_STR
    from data import load_data_with_cache, load_smart_money_data
    from viz import plot_single_timeframe, plot_smart_money
    from smc_core import (
        ensure_smc_columns, compute_smc_levels, detect_fvg_zones,
        detect_order_blocks, detect_trendlines, detect_confluence_zones
    )
    import smc_core 
    from scanner import scan_symbol, scan_universe_two_phase, process_and_send_vnindex_report, export_journal, format_scan_report
    from indicators import detect_rsi_divergence
    from pipeline_manager import run_bulk_update
    from telegram_bot import send_telegram_msg

    # Trả về tất cả các biến/hàm cần dùng dưới dạng tuple
    return (
        pd, go, concurrent, datetime, ZoneInfo,
        get_vnallshare_universe, load_data_with_cache, load_smart_money_data,
        plot_single_timeframe, plot_smart_money,
        ensure_smc_columns, compute_smc_levels, detect_fvg_zones, detect_order_blocks, detect_trendlines, detect_confluence_zones,
        scan_symbol, scan_universe_two_phase, process_and_send_vnindex_report, export_journal, format_scan_report,
        detect_rsi_divergence, run_bulk_update, smc_core, send_telegram_msg,
        RAW_TICKERS_STR 
    )

@st.cache_data(ttl=3600*12)
def get_sector_map():
    """Lấy mapping Mã CK -> Tên Ngành từ nguồn VCI"""
    try:
        from vnstock_data import Listing
        listing = Listing(source='vci')
        df = listing.symbols_by_industries(lang='vi')
        if not df.empty and 'symbol' in df.columns and 'icb_name3' in df.columns:
            return dict(zip(df['symbol'], df['icb_name3']))
    except Exception as e:
        pass
    return {}

# =========================
# LOAD MODULES VÀO BIẾN TOÀN CỤC
# =========================
# Gọi hàm load modules. Lần đầu sẽ tốn vài giây, các lần sau tức thì.
vars_loaded = init_modules()

# BUNG NÉN BIẾN RA ĐỂ DÙNG (UNPACKING)
(
    pd, go, concurrent, datetime, ZoneInfo,
    get_vnallshare_universe, load_data_with_cache, load_smart_money_data,
    plot_single_timeframe, plot_smart_money,
    ensure_smc_columns, compute_smc_levels, detect_fvg_zones, detect_order_blocks, detect_trendlines, detect_confluence_zones,
    scan_symbol, scan_universe_two_phase, process_and_send_vnindex_report, export_journal, format_scan_report,
    detect_rsi_divergence, run_bulk_update, smc_core, send_telegram_msg,
    RAW_TICKERS_STR 
) = vars_loaded

# Cập nhật lại danh sách Scan đầy đủ từ Universe (nếu đang dùng list mặc định ngắn)
if "scan_symbols_text" in st.session_state and len(st.session_state.scan_symbols_text) < 200:
     st.session_state.scan_symbols_text = RAW_TICKERS_STR.replace("\n", " ").strip()


# ==============================================================================
# 3. LOGIC XỬ LÝ SỰ KIỆN SIDEBAR (ĐÃ LOAD XONG THƯ VIỆN)
# ==============================================================================

# Xử lý nút BC VNINDEX (Giờ mới xử lý vì cần thư viện scan)
if btn_vnindex:
    with st.spinner("Đang phân tích và gửi báo cáo..."):
        success, msg = process_and_send_vnindex_report()
        if success: st.sidebar.success("Đã gửi!")
        else: st.sidebar.error(f"Lỗi: {msg}")

st.caption("Giờ Server: " + datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%H:%M:%S"))

def core_healthcheck_ui():
    issues = []
    must = ["compute_smc_levels", "entry_breaker_retest"]
    for n in must:
        if not hasattr(smc_core, n): issues.append(f"Missing {n}")
    return issues

@st.cache_data(ttl=600, show_spinner=False)
def load_smart_money_cached_wrapper(symbol: str):
    return load_smart_money_data(symbol)

def plotly_draw_config():
    return {
        "scrollZoom": True, "displayModeBar": True, "displaylogo": False,
        "modeBarButtonsToRemove": ["lasso2d", "select2d"]
    }

# ==============================================================================
# 4. HÀM VẼ CHART (GIỮ NGUYÊN LOGIC CŨ)
# ==============================================================================
# --- [TỐI ƯU HÓA] CACHE TÍNH TOÁN SMC ---
@st.cache_data(ttl=300, max_entries=20, show_spinner=False)
def calculate_smc_cached(df_json_orient_split, use_rsi, use_trendline):
    """
    Hàm wrapper để cache các tính toán nặng (SMC, FVG, OB, Trendlines).
    Truyền df dưới dạng JSON hoặc Dictionary để Streamlit hash được nhanh hơn.
    """
    # Reconstruct DataFrame từ JSON/Dict để tính toán
    import pandas as pd
    df = pd.DataFrame(**df_json_orient_split) 
    df['Date'] = pd.to_datetime(df['Date']) if 'Date' in df.columns else df.index
    if 'Date' in df.columns: df.set_index('Date', inplace=True)

    # Tính toán (Logic cũ của bạn)
    smc = compute_smc_levels(df)
    fvgs = detect_fvg_zones(df, max_zones=5)
    obs = detect_order_blocks(df)
    fvgs, obs = detect_confluence_zones(df, fvgs, obs)
    rsi_divs = detect_rsi_divergence(df, lookback=100) if use_rsi else []
    t_lines = detect_trendlines(df) if use_trendline else []
    
    return smc, fvgs, obs, rsi_divs, t_lines
    
def process_and_plot(
    df, interval, show_vol_param=True, show_ma_param=True, show_vsa_param=False,
    htf_zones=None, skip_current_zones=False, enable_smart_money=False, build_fig=True,
):
    if htf_zones is None: htf_zones = []
    if df is None or df.empty: return None, []

    current_sym = st.session_state.current_symbol
    df = ensure_smc_columns(df)

    # 1) Smart money
    df_smart_money = None
    if enable_smart_money:
        try: df_smart_money = load_smart_money_cached_wrapper(current_sym)
        except Exception: df_smart_money = None

    # 2) Tính zones/levels
    # Chuyển DF thành dict để làm key cho cache (nhanh hơn hash cả dataframe lớn)
    df_serialized = df.reset_index().to_dict(orient='split') 
    
    # Gọi hàm đã cache ở Bước 2.1
    smc, fvgs_raw, obs_raw, rsi_divs, t_lines = calculate_smc_cached(
        df_serialized, use_rsi, use_trendline
    )

    plot_fvgs = [] if skip_current_zones else fvgs_raw
    plot_obs = [] if skip_current_zones else obs_raw
    zones_out = fvgs_raw + obs_raw

    # 3) Plot
    if not build_fig: return None, zones_out

    fig = plot_single_timeframe(
        df, current_sym, interval,
        smc_levels=smc, fvg_zones=plot_fvgs, ob_zones=plot_obs,
        htf_zones=htf_zones, trendlines=t_lines, rsi_divergences=rsi_divs,
        show_vol=show_vol_param, show_ma=(show_ma_param and use_ma),
        show_vsa=show_vsa_param, smart_money_data=df_smart_money,
        show_rsi=use_rsi, show_smc=use_smc,
    )
    return fig, zones_out

# ==============================================================================
# 5. DASHBOARD CHÍNH
# ==============================================================================
st.title(f"📊 {st.session_state.current_symbol}")
symbol = st.session_state.current_symbol

# ---- GATE CHECK ----
if "dashboard_loaded" not in st.session_state:
    st.session_state.dashboard_loaded = False

col_gate1, col_gate2 = st.columns([1, 3])
with col_gate1:
    if st.button("📥 Tải & Vẽ Chart", type="primary"):
        st.session_state.dashboard_loaded = True
with col_gate2:
    st.info("Bấm nút để tải dữ liệu (Giúp App khởi động nhanh).")

if not st.session_state.dashboard_loaded:
    st.stop()

# ---- DATA LOADING ----
df_1d = load_data_with_cache(symbol, days_to_load=365, timeframe="1D")

if df_1d is not None and not df_1d.empty:
    last = df_1d.iloc[-1]
    prev = df_1d.iloc[-2] if len(df_1d) > 1 else last
    chg = last["Close"] - prev["Close"]
    pct = (chg / prev["Close"]) * 100 if prev["Close"] != 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Close", f"{last['Close']:,.2f}", f"{chg:,.2f} ({pct:.2f}%)")
    c2.metric("Vol", f"{last['Volume']:,.0f}")
    c3.metric("RSI", f"{last.get('RSI_14', 0):.2f}")
    
    ma20 = last.get("SMA_20", 0)
    ema50 = last.get("EMA_50", 0)
    trend = "UP 🚀" if last["Close"] > ma20 else "DOWN 🐻"
    if last["Close"] > ma20 and last["Close"] < ema50: trend = "SIDEWAY"
    c4.metric("Trend", trend)

    tf_choice = st.radio("Khung thời gian", ["Daily (1D)", "Hourly (1H)", "15 Minutes"], horizontal=True)

    # 1. Khởi tạo biến Session State nếu chưa có (để tránh lỗi khi gọi)
    if "d1_zones" not in st.session_state: st.session_state.d1_zones = []
    if "h1_zones" not in st.session_state: st.session_state.h1_zones = []

    # 2. Xử lý logic theo Khung thời gian
    if tf_choice == "Daily (1D)":
        with st.spinner("Vẽ 1D..."):
            fig_d1, d1_zones = process_and_plot(
                df_1d, "1D", 
                show_vol_param=use_vol, show_ma_param=use_ma,
                show_vsa_param=use_vsa, htf_zones=[], skip_current_zones=False,
                enable_smart_money=use_smart_money, build_fig=True,
            )
            st.session_state.d1_zones = d1_zones
        if fig_d1: st.plotly_chart(fig_d1, width='stretch', config=plotly_draw_config())

    elif tf_choice == "Hourly (1H)":
        # Đảm bảo đã có Zone D1 trước khi vẽ H1
        if not st.session_state.d1_zones:
            _, st.session_state.d1_zones = process_and_plot(df_1d, "1D", build_fig=False)

        df_1h = load_data_with_cache(symbol, days_to_load=200, timeframe="1H")
        if df_1h is not None and not df_1h.empty:
            with st.spinner("Vẽ 1H..."):
                fig_h1, h1_zones = process_and_plot(
                    df_1h, "1H", 
                    # Bật các chỉ báo cho H1 (như D1)
                    show_vol_param=use_vol,      
                    show_ma_param=use_ma,        
                    show_vsa_param=use_vsa,      
                    htf_zones=st.session_state.d1_zones,
                    skip_current_zones=False, 
                    enable_smart_money=use_smart_money, 
                    build_fig=True,
                )
                st.session_state.h1_zones = h1_zones
            if fig_h1: st.plotly_chart(fig_h1, width='stretch', config=plotly_draw_config())
        else: 
            st.warning("Chưa có data 1H.")

    else: # 15 Minutes
        # Đảm bảo đã có Zone D1
        if not st.session_state.d1_zones:
            _, st.session_state.d1_zones = process_and_plot(df_1d, "1D", build_fig=False)
        
        # --- [QUAN TRỌNG] Khởi tạo h1_zones rỗng để tránh NameError ---
        h1_zones = [] 
        
        # Logic Overlay H1 (Nếu user tích chọn thì mới tính toán)
        use_h1_overlay = st.checkbox("Overlay zones 1H", value=False)
        
        if use_h1_overlay:
            # Ưu tiên lấy từ cache session nếu đã có
            if st.session_state.h1_zones:
                h1_zones = st.session_state.h1_zones
            else:
                # Nếu chưa có thì load data H1 tính nóng
                df_1h_temp = load_data_with_cache(symbol, days_to_load=200, timeframe="1H")
                if df_1h_temp is not None and not df_1h_temp.empty:
                    _, h1_zones = process_and_plot(df_1h_temp, "1H", build_fig=False)
                    st.session_state.h1_zones = h1_zones # Lưu lại cho lần sau

        # Tải dữ liệu 15m
        df_15m = load_data_with_cache(symbol, days_to_load=400, timeframe="15m")
        
        if df_15m is not None and not df_15m.empty:
            # Gộp zone: D1 + H1 (nếu có)
            final_htf = list(st.session_state.d1_zones) + list(h1_zones)
            
            with st.spinner("Vẽ 15m..."):
                fig_15, _ = process_and_plot(
                    df_15m, "15m", 
                    # Tắt hết chỉ báo phụ ở 15m cho nhẹ
                    show_vol_param=False, 
                    show_ma_param=False, 
                    show_vsa_param=False,
                    enable_smart_money=False,
                    htf_zones=final_htf, 
                    skip_current_zones=False, # Vẽ zone 15m
                    build_fig=True,
                )
            if fig_15: st.plotly_chart(fig_15, width='stretch', config=plotly_draw_config())
        else: 
            st.warning("Chưa có data 15m.")


# ============================================================================
# ==============================================================================
# 6. SMC SCANNER (Dashboard 2 cột)
# ==============================================================================
st.markdown("---")
st.subheader("🚀 SMC Scanner")

# ---------- CSS: Dark + Accent theo Signal ----------
st.markdown("""
<style>
.block-container {padding-top: 1.1rem;}
.card{
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 16px;
  padding: 14px 14px;
}
.badge{
  display:inline-block;
  padding: 2px 10px;
  border-radius: 999px;
  border:1px solid rgba(255,255,255,0.12);
  background: rgba(255,255,255,0.04);
  font-size: 0.78rem;
  opacity:0.92;
}
.hint{opacity:0.78; font-size: 0.86rem; margin-top: 2px;}
/* Primary button tint */
div.stButton > button[kind="primary"]{
  background: linear-gradient(90deg, rgba(239,68,68,0.95), rgba(249,115,22,0.95));
  border: 0px;
}
div.stButton > button[kind="primary"]:hover{
  filter: brightness(1.05);
}
/* Signal styles in dataframe */
td.signal-buy {color:#22C55E !important; font-weight:800;}
td.signal-sell {color:#F87171 !important; font-weight:800;}
</style>
""", unsafe_allow_html=True)

# ---------- Session state ----------
if "scan_symbols_text" not in st.session_state:
    st.session_state.scan_symbols_text = default_str
if "cache_ready" not in st.session_state:
    st.session_state.cache_ready = False
if "last_cache_update" not in st.session_state:
    st.session_state.last_cache_update = None
if "scan_rejected" not in st.session_state:
    st.session_state.scan_rejected = []

# Parse symbols helper
def _parse_symbols(txt: str):
    raw = (txt or "").replace("\n", " ").replace(",", " ").replace(";", " ")
    return [s.strip().upper() for s in raw.split() if s.strip()]

# ---------- Sidebar Control Panel (ĐÃ TỐI ƯU GỌN NHẸ) ----------
with st.sidebar:
    st.markdown("## ⚙️ Control Panel")
    
    # CSS nhỏ để giảm khoảng cách giữa các phần tử trong Sidebar
    st.markdown("""
        <style>
        [data-testid="stSidebar"] .stButton {margin-bottom: 0px;}
        .card {margin-bottom: 10px; padding: 10px;} /* Giảm margin card */
        </style>
    """, unsafe_allow_html=True)

    # --- 1. Load Universe (Chỉ còn đúng 1 nút bấm nằm trong card) ---
    #st.markdown('<div class="card">', unsafe_allow_html=True)
    if st.button("🌍1.Load Universe", width='stretch'):
        with st.spinner("Loading..."):
            try:
                uni_list = get_vnallshare_universe(days=20)
                if uni_list:
                    st.session_state.scan_symbols_text = ", ".join(uni_list)
                    st.session_state.cache_ready = False
                    st.success(f"OK: {len(uni_list)} mã")
            except Exception as e: st.error("Lỗi mạng")
    st.markdown('</div>', unsafe_allow_html=True)

    # --- 2. Update Cache (Gọn nhẹ) ---
    #st.markdown('<div class="card">', unsafe_allow_html=True)
    scan_symbols_sidebar = _parse_symbols(st.session_state.scan_symbols_text)
    if st.button("📥2.Update Cache", width='stretch'):
        if not scan_symbols_sidebar: st.error("List trống")
        else:
            with st.status("Updating...", expanded=True) as status:
                res = run_bulk_update(scan_symbols_sidebar, days_back=3)
                if "Lỗi" not in res:
                    status.update(label="Done!", state="complete", expanded=False)
                    st.session_state.cache_ready = True
                    st.session_state.last_cache_update = datetime.now().strftime("%H:%M")
                    st.toast("Updated!", icon="💾")
                else: st.error(res)
    st.markdown('</div>', unsafe_allow_html=True)

    # --- 3. Start Scan (Tinh gọn nhất) ---
    #   st.markdown('<div class="card">', unsafe_allow_html=True)
    
    # Gom Slider và Checkbox vào 2 cột cho tiết kiệm dòng
    c1, c2 = st.columns([2, 1.5]) 
    with c1:
        # label_visibility="collapsed" giúp ẩn chữ "Shortlist" đi nếu muốn siêu gọn
        # hoặc giữ "visible" nhưng chỉnh margin
        shortlist_n = st.slider("Top", 50, 100, 100, 10, help="Số lượng mã lọc phase 1")
    with c2:
        auto_send_tele = st.checkbox("Tele", value=False)

    start_disabled = not st.session_state.get("cache_ready", False)
    # Nút Scan
    # Nút Scan (Gán vào biến start_scan để bên dưới dùng được)
    start_scan = st.button("🔥3.SCAN", type="primary", width='stretch', disabled=start_disabled)
        # Logic Scan được xử lý ở main dashboard, nút này chỉ trigger rerun 
        # (Thực tế code cũ nút này nằm trong form hoặc biến start_scan sẽ được dùng ở dưới)
        #pass 
    st.markdown('</div>', unsafe_allow_html=True)

# ---------- Main: Dashboard (Top: Results, Bottom: Chart) ----------
# Top: List/Filters/Results
with st.expander("🧾 List Scan", expanded=False):
    scan_list_input = st.text_area("List Scan", value=st.session_state.scan_symbols_text, height=110)
    if scan_list_input != st.session_state.scan_symbols_text:
        st.session_state.scan_symbols_text = scan_list_input
        st.session_state.cache_ready = False

#issues = core_healthcheck_ui()
#if issues:
#    st.warning(f"Core Check: {issues}")
#else:
#    st.success("Core OK ✅")
sector_map = get_sector_map()
st.markdown("### 🔎 Filters")

f1, f2, f3, f4 = st.columns([1, 1, 1, 1])
with f1:
    signal_filter = st.selectbox("Signal", ["ALL", "BUY", "SELL"], index=0)
with f2:
    min_score = st.number_input("Score >=", value=0.0, step=0.5)
with f3:
    # Lấy danh sách ngành thực tế để đưa vào Selectbox
    available_sectors = ["ALL"]
    if sector_map:
        available_sectors += sorted(list(set(sector_map.values())))
    sector_filter = st.selectbox("Sector", available_sectors, index=0)
with f4:
    # THÊM BỘ LỌC R:R
    min_rr = st.number_input("R:R >=", value=0.0, step=0.5, help="Tỷ lệ Reward/Risk tối thiểu")
# Run scan when clicked
if start_scan:
    st.session_state.scan_results = None
    st.session_state.scan_rejected = []

    scan_symbols = _parse_symbols(st.session_state.scan_symbols_text)

    with st.status("🔎 Scanning 2-phase (D1 → 1H/15m)...", expanded=True) as status:
        try:
            results, rejected = scan_universe_two_phase(
                scan_symbols,
                days=60,
                ema_span=50,
                nav=input_nav,
                risk_pct=input_risk,
                max_positions=input_max_pos,
                shortlist_n=shortlist_n,
                max_workers_phase1=16,
                max_workers_phase2=10,
            )
            st.session_state.scan_rejected = rejected

            if results:
                df_res = pd.DataFrame(results)
                df_res.sort_values(
                    by=["Signal", "Score", "Symbol"],
                    ascending=[True, False, True],
                    inplace=True
                )
                st.session_state.scan_results = df_res
                status.update(
                    label=f"✅ Found {len(df_res)} setups!",
                    state="complete",
                    expanded=False
                )
            else:
                status.update(
                    label="⚠️ No setup found.",
                    state="complete",
                    expanded=False
                )

        except Exception as e:
            status.update(label="❌ Scan lỗi", state="error", expanded=True)
            st.exception(e)

    # ✅ GỬI TELEGRAM – ĐẶT NGOÀI TRY/EXCEPT
    if auto_send_tele and st.session_state.get("scan_results") is not None:
        msg = format_scan_report(st.session_state.scan_results)
        if not msg.startswith("⚠️"):
            send_telegram_msg(msg)


# Results table
if st.session_state.get("scan_results") is not None and not st.session_state.scan_results.empty:
    df_res = st.session_state.scan_results.copy()

    # A. MAP SECTOR VÀO DATAFRAME
    # Nếu trong scanner chưa có Sector, ta map từ sector_map vào
    if "Sector" not in df_res.columns:
        df_res["Sector"] = df_res["Symbol"].map(sector_map).fillna("Khác")

    # B. ÁP DỤNG BỘ LỌC
    dff = df_res.copy()
    
    # 1. Lọc Signal
    if signal_filter != "ALL":
        dff = dff[dff["Signal"].astype(str).str.contains(signal_filter)]
    
    # 2. Lọc Score
    try:
        dff = dff[dff["Score"].astype(float) >= float(min_score)]
    except: pass
    
    # 3. Lọc Sector
    if sector_filter != "ALL":
        dff = dff[dff["Sector"] == sector_filter]

    # 4. Lọc R:R (Mới)
    if "RR" in dff.columns:
        try:
            dff = dff[dff["RR"].astype(float) >= float(min_rr)]
        except: pass

    # KPI Metrics
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Setups", len(dff))
    k2.metric("BUY", int(dff["Signal"].astype(str).str.contains("BUY").sum()))
    k3.metric("SELL", int(dff["Signal"].astype(str).str.contains("SELL").sum()))
    try:
        # Hiển thị R:R trung bình thay vì Score (hoặc tùy bạn chọn)
        avg_rr = dff["RR"].mean() if "RR" in dff.columns else 0
        k4.metric("Avg R:R", f"{avg_rr:.2f}") 
    except:
        k4.metric("Avg R:R", "-")
        
    # Export
    b1, b2 = st.columns([1,1])
    with b1:
        if st.button("📒 Export Journal", width='stretch'):
            df_j = export_journal(dff)
            if df_j is not None:
                st.dataframe(df_j, hide_index=True, width='stretch')
    with b2:
        if st.button("📤 Gửi Tele", width='stretch'):
            msg = format_scan_report(dff)
            send_telegram_msg(msg)
            st.toast("Sent!")

    # --- STYLE FUNCTIONS (ĐÃ THÊM MỚI TẠI ĐÂY) ---
    def format_score_ui(val):
        try: v = float(val)
        except: v = 0.0
        if v >= 4.0: return f"🔥🔥🔥 {v}"
        if v >= 3.0: return f"⭐⭐ {v}"
        return str(v)

    def _style_signal(val):
        sval = str(val)
        if "BUY" in sval: return "color:#22C55E; font-weight:800"
        if "SELL" in sval: return "color:#F87171; font-weight:800"
        return ""

    # === LOGIC TÔ MÀU CHỮ DIST% ===
    def _style_dist_poi(val):
        try:
            v = float(val)
            abs_v = abs(v)
            # Từ -2% đến 2%: Màu Xanh
            if abs_v <= 2.0: 
                return "color: #00E676; font-weight: 700" 
            
            # Từ 2% đến 5% (hoặc -5% đến -2%): Màu Vàng
            if 2.0 < abs_v <= 5.0: 
                return "color: #FFD700; font-weight: 700" 
            
            # Còn lại (Xa hơn 5%): Màu Trắng (Mặc định)
            return "" 
        except Exception: 
            return ""

    # C. HIỂN THỊ BẢNG (Cập nhật cột và sắp xếp)
    st.markdown("### 📋 Results")
    
    # Tạo style tô màu
    dff["Display_Score"] = dff["Score"].apply(format_score_ui)

    # Cấu hình thứ tự cột (Đưa Sector và RR lên cho dễ nhìn)
    # Thứ tự: Symbol -> Signal -> RR -> Score -> Sector -> ...
    cols_order = [ "Symbol", "Sector", "Signal", "Display_Score",  "RR", "Dist_POI", "Price", "POI_D1", "KL", "SL", "TP", "Note"]
    # Chỉ lấy những cột thực sự có trong dff
    final_cols = [c for c in cols_order if c in dff.columns]

    event = st.dataframe(
        dff.style.map(_style_signal, subset=["Signal"])
                 .map(_style_dist_poi, subset=["Dist_POI"]),
        width='stretch',
        hide_index=True,
        column_order=final_cols,
        column_config={
            "RR": st.column_config.NumberColumn("R:R", format="%.2f", help="Risk:Reward Ratio"),         
            "Display_Score": st.column_config.TextColumn("Score", width="medium"),             
            "Sector": st.column_config.TextColumn("Ngành", width="medium"),
            "Dist_POI": st.column_config.NumberColumn("Dist%", format="%.2f%%"),
            "Price": st.column_config.NumberColumn("Price", format="%.2f"),
            "POI_D1": st.column_config.NumberColumn("POI", format="%.2f"),
            "SL": st.column_config.NumberColumn("SL", format="%.2f"),
        },
        on_select="rerun",
        selection_mode="single-row"
    )

    if len(event.selection.rows) > 0:
        sel_idx = event.selection.rows[0]
        new_sym = dff.iloc[sel_idx]["Symbol"]
        if st.session_state.get("current_symbol") != new_sym:
            st.session_state.current_symbol = new_sym

else:
    st.info("Chưa có kết quả. Hãy Update Cache → Start Scan.")

# Rejected log
if st.session_state.get("scan_rejected"):
    with st.expander(f"🧨 Rejected ({len(st.session_state.scan_rejected)})", expanded=False):
        st.dataframe(pd.DataFrame(st.session_state.scan_rejected, columns=["Symbol","Reason"]),
                     hide_index=True, width='stretch')


st.markdown("---")

# Bottom: Chart (full width)  [DISABLED to avoid double-render]
st.markdown("### 📈 Chart")
st.caption("📊 Chart đang hiển thị ở Dashboard phía trên. (Click mã trong bảng Results để đổi chart.)")


# =============================
# 7. MINI BOT
# ==============================================================================
st.sidebar.markdown("---")
bot_query = st.sidebar.text_input("Bot Tra Cứu", placeholder="Mã...", label_visibility="collapsed").upper().strip()
if bot_query:
    with st.sidebar.status(f"Soi {bot_query}...", expanded=True) as status:
        try:
            res, reason = scan_symbol(bot_query, days=100)
            if res:
                status.update(label="✅ Có tín hiệu!", state="complete")
                st.sidebar.write(f"**{res['Signal']}** | Score: {res['Score']}")
                if st.sidebar.button("Xem"):
                    st.session_state.current_symbol = bot_query
                    st.rerun()
            else:
                status.update(label="zzz", state="complete")
                st.sidebar.caption(reason)
        except Exception: st.sidebar.error("Lỗi")