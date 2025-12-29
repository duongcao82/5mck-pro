import os
import sys
import time
import streamlit as st

# ==============================================================================
# 1. SETUP CƠ BẢN & UI SKELETON (CHẠY NGAY LẬP TỨC)
# ==============================================================================
# BẮT BUỘC: Lệnh này phải ở dòng đầu tiên
st.set_page_config(page_title="5MCK Pro", layout="wide", page_icon="📈")

# Fix path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# --- A. VẼ GIAO DIỆN CƠ BẢN TRƯỚC (Để qua mặt Healthcheck 503) ---
# Chúng ta vẽ Sidebar và Tiêu đề TRƯỚC KHI load thư viện nặng
st.markdown(
    """
    <style>
        .stApp { background-color: #0e1117; color: white; } 
        .metric-card { background-color: #262730; padding: 10px; border-radius: 5px; margin-bottom: 10px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Init Session State cơ bản
if "current_symbol" not in st.session_state:
    st.session_state.current_symbol = "VNINDEX"

# --- B. VẼ SIDEBAR (Dùng thư viện chuẩn, chưa cần Pandas/Vnstock) ---
st.sidebar.empty()
current_dir = os.path.dirname(os.path.abspath(__file__))
img_logo = os.path.join(current_dir, "5MCK_Logo.png")
img_bidv = os.path.join(current_dir, "5MCK_BIDV.png")
img_vps = os.path.join(current_dir, "5MCK_VPS.jpg")

if os.path.exists(img_logo):
    try: st.sidebar.image(img_logo, width="stretch")
    except: st.sidebar.title("🎛️ 5MCK Control")
else:
    st.sidebar.title("🎛️ 5MCK Control")

st.sidebar.write("")
col_logo1, col_logo2 = st.sidebar.columns(2)
with col_logo1:
    if os.path.exists(img_bidv):
        try: st.sidebar.image(img_bidv, width="stretch")
        except: pass
with col_logo2:
    if os.path.exists(img_vps):
        try: st.sidebar.image(img_vps, width="stretch")
        except: pass

st.sidebar.markdown("---")

# INPUT CƠ BẢN
symbol_input = st.sidebar.text_input("🔍 Tra cứu Mã:", value=st.session_state.current_symbol).upper()
if symbol_input != st.session_state.current_symbol:
    st.session_state.current_symbol = symbol_input
    st.rerun()

# CÁC NÚT ĐIỀU KHIỂN (Vẫn vẽ được dù chưa có logic xử lý)
st.sidebar.markdown("---")
btn_vnindex = st.sidebar.button("📢 BC VNINDEX")

st.sidebar.markdown("---")
st.sidebar.subheader("💰 SMC Money Management")
input_nav = st.sidebar.number_input("Tổng vốn (NAV)", value=1_000_000_000, step=100_000_000)
input_risk = st.sidebar.slider("Rủi ro mỗi lệnh (%)", 0.1, 5.0, 1.0) / 100
input_max_pos = st.sidebar.number_input("Số lệnh tối đa", value=5, step=1)

st.sidebar.subheader("⚙️ Cấu hình Chart")
use_ma = st.sidebar.checkbox("MAs", value=False)
use_vsa = st.sidebar.checkbox("VSA Signals", value=False)
use_rsi = st.sidebar.checkbox("RSI", value=True)
use_smc = st.sidebar.checkbox("SMC Zones", value=True)
use_vol = st.sidebar.checkbox("Volume", value=True)
use_trendline = st.sidebar.checkbox("Trendlines", value=True)
use_smart_money = st.sidebar.checkbox("💰 Smart Money", value=False)


# ==============================================================================
# 2. HEAVY LOADING (BÂY GIỜ MỚI LOAD THƯ VIỆN NẶNG)
# ==============================================================================

@st.cache_resource(show_spinner=False)
def init_modules():
    import concurrent.futures
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import pandas as pd
    import plotly.graph_objects as go

    # Config Pandas
    pd.set_option("future.no_silent_downcasting", True)

    # Import Modules nội bộ
    from universe import get_vnallshare_universe
    from data import load_data_with_cache, load_smart_money_data
    from viz import plot_single_timeframe, plot_smart_money
    from smc_core import (
        ensure_smc_columns, compute_smc_levels, detect_fvg_zones,
        detect_order_blocks, detect_trendlines, detect_confluence_zones
    )
    from scanner import scan_symbol, process_and_send_vnindex_report, export_journal, format_scan_report
    from indicators import detect_rsi_divergence
    from pipeline_manager import run_bulk_update
    import smc_core 
    from telegram_bot import send_telegram_msg

    return (
        pd, go, concurrent, datetime, ZoneInfo,
        get_vnallshare_universe, load_data_with_cache, load_smart_money_data,
        plot_single_timeframe, plot_smart_money,
        ensure_smc_columns, compute_smc_levels, detect_fvg_zones, detect_order_blocks, detect_trendlines, detect_confluence_zones,
        scan_symbol, process_and_send_vnindex_report, export_journal, format_scan_report,
        detect_rsi_divergence, run_bulk_update, smc_core, send_telegram_msg
    )

# HIỂN THỊ LOADING SPINNER Ở CHÍNH GIỮA MÀN HÌNH
if "modules_loaded" not in st.session_state:
    with st.spinner("🚀 Đang khởi động hệ thống AI (Lần đầu mất khoảng 30s)..."):
        # Gọi hàm load ở đây
        vars_loaded = init_modules()
        st.session_state.vars_loaded = vars_loaded
        st.session_state.modules_loaded = True
else:
    vars_loaded = st.session_state.vars_loaded

# UNPACK VARIABLES (Bung nén biến ra để dùng)
(
    pd, go, concurrent, datetime, ZoneInfo,
    get_vnallshare_universe, load_data_with_cache, load_smart_money_data,
    plot_single_timeframe, plot_smart_money,
    ensure_smc_columns, compute_smc_levels, detect_fvg_zones, detect_order_blocks, detect_trendlines, detect_confluence_zones,
    scan_symbol, process_and_send_vnindex_report, export_journal, format_scan_report,
    detect_rsi_divergence, run_bulk_update, smc_core, send_telegram_msg
) = vars_loaded

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
    smc = compute_smc_levels(df)
    fvgs = detect_fvg_zones(df, max_zones=5)
    obs = detect_order_blocks(df)
    fvgs, obs = detect_confluence_zones(df, fvgs, obs)
    rsi_divs = detect_rsi_divergence(df, lookback=100) if use_rsi else []
    t_lines = detect_trendlines(df) if use_trendline else []

    plot_fvgs = [] if skip_current_zones else fvgs
    plot_obs = [] if skip_current_zones else obs
    zones_out = fvgs + obs

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

    if "d1_zones" not in st.session_state: st.session_state.d1_zones = []
    if "h1_zones" not in st.session_state: st.session_state.h1_zones = []

    if tf_choice == "Daily (1D)":
        with st.spinner("Vẽ 1D..."):
            fig_d1, d1_zones = process_and_plot(
                df_1d, "1D", show_vol_param=use_vol, show_ma_param=use_ma,
                show_vsa_param=use_vsa, htf_zones=[], skip_current_zones=False,
                enable_smart_money=use_smart_money, build_fig=True,
            )
            st.session_state.d1_zones = d1_zones
        if fig_d1: st.plotly_chart(fig_d1, use_container_width=True, config=plotly_draw_config())

    elif tf_choice == "Hourly (1H)":
        if not st.session_state.d1_zones:
            _, st.session_state.d1_zones = process_and_plot(df_1d, "1D", build_fig=False)

        df_1h = load_data_with_cache(symbol, days_to_load=200, timeframe="1H")
        if df_1h is not None and not df_1h.empty:
            with st.spinner("Vẽ 1H..."):
                fig_h1, h1_zones = process_and_plot(
                    df_1h, "1H", show_vol_param=False, show_ma_param=False,
                    show_vsa_param=False, htf_zones=st.session_state.d1_zones,
                    skip_current_zones=False, enable_smart_money=use_smart_money, build_fig=True,
                )
                st.session_state.h1_zones = h1_zones
            if fig_h1: st.plotly_chart(fig_h1, use_container_width=True, config=plotly_draw_config())
        else: st.warning("Chưa có data 1H.")

    else:
        if not st.session_state.d1_zones:
            _, st.session_state.d1_zones = process_and_plot(df_1d, "1D", build_fig=False)
        
        use_h1_overlay = st.checkbox("Overlay zones 1H", value=False)
        h1_zones = []
        if use_h1_overlay:
            df_1h = load_data_with_cache(symbol, days_to_load=200, timeframe="1H")
            if df_1h is not None and not df_1h.empty:
                _, h1_zones = process_and_plot(df_1h, "1H", htf_zones=st.session_state.d1_zones, enable_smart_money=False, build_fig=False)
                for z in h1_zones: z["is_from_1h"] = True
        
        df_15m = load_data_with_cache(symbol, days_to_load=400, timeframe="15m")
        if df_15m is not None and not df_15m.empty:
            final_htf = list(st.session_state.d1_zones) + list(h1_zones)
            with st.spinner("Vẽ 15m..."):
                fig_15, _ = process_and_plot(
                    df_15m, "15m", show_vol_param=False, show_ma_param=False,
                    show_vsa_param=False, htf_zones=final_htf, skip_current_zones=True,
                    enable_smart_money=use_smart_money, build_fig=True,
                )
            if fig_15: st.plotly_chart(fig_15, use_container_width=True, config=plotly_draw_config())
        else: st.warning("Chưa có data 15m.")
else:
    st.error(f"⚠️ Chưa có dữ liệu {symbol}.")


# ==============================================================================
# 6. SMC SCANNER
# ==============================================================================
st.markdown("---")
st.subheader("🚀 SMC Scanner")

default_str = """ACB, AGR, ANV, BAF, BCM, BID, BMP, BSI, BVH, CII, CTD, CTG, CTR, CTS, DBC, DCL, DCM, DGC, DGW, DHA, DIG, DPG, DPM, DXG, EIB, ELC, EVF, FCN, FPT, FRT, FTS, GAS, GEX, GMD, GVR, HAG, HAH, HCM, HDB, HDC, HDG, HHS, HHV, HPG, HSG, IJC, KBC, KDC, KDH, KOS, KSB, LCG, LPB, MBB, MSB, MSN, MWG, NAB, NAF, NKG, NLG, NT2, NTL, OCB, ORS, PAN, PC1, PDR, PET, PHR, PLX, PNJ, POW, PVD, PVT, QCG, REE, SAB, SBT, SCS, SHB, SHI, SIP, SSB, SSI, STB, TCB, TCH, TCM, TLG, TPB, VCB, VCG, VCI, VDS, VGC, VHC, VHM, VIB, VIC, VIX, VJC, VND, VNM, VPB, VPI, VRE, VSC, VTP, YEG"""

if "scan_symbols_text" not in st.session_state:
    st.session_state.scan_symbols_text = default_str

col_u1, col_u2 = st.columns([1, 3])
with col_u1:
    if st.button("🌍 Load VN-Universe"):
        with st.spinner("Đang lọc Universe..."):
            try:
                uni_list = get_vnallshare_universe(days=20)
                if uni_list:
                    st.session_state.scan_symbols_text = ", ".join(uni_list)
                    st.success(f"Load {len(uni_list)} mã!")
                    time.sleep(1)
                    st.rerun()
            except Exception as e: st.error(str(e))

with col_u2:
    st.info("Universe: Các mã thanh khoản cao lọc từ VNALLSHARE.")

scan_list_input = st.text_area("List Scan:", value=st.session_state.scan_symbols_text, height=100)
if scan_list_input != st.session_state.scan_symbols_text:
    st.session_state.scan_symbols_text = scan_list_input

raw_symbols = scan_list_input.replace("\n", " ").replace(",", " ").replace(";", " ")
scan_symbols = [s.strip().upper() for s in raw_symbols.split(" ") if s.strip()]

issues = core_healthcheck_ui()
if issues: st.warning(f"Core Check: {issues}")

c_btn1, c_btn2 = st.columns(2)
with c_btn1:
    st.write("1️⃣ **Bước 1: Update Cache**")
    if st.button("📥 Cập nhật Dữ liệu", width="stretch"):
        if not scan_symbols: st.error("List trống!")
        else:
            with st.status("⏳ Đang tải dữ liệu...", expanded=True) as status:
                # Gọi hàm từ pipeline_manager
                res = run_bulk_update(scan_symbols, days_back=365)
                if "Lỗi" not in res:
                    status.update(label="✅ Đã cập nhật Cache!", state="complete", expanded=False)
                    st.toast("Done!", icon="💾")
                    time.sleep(1)
                    st.rerun()
                else:
                    status.update(label="❌ Lỗi", state="error")
                    st.error(res)

with c_btn2:
    st.write("2️⃣ **Bước 2: Tìm cơ hội**")
    auto_send_tele = st.checkbox("Auto Telegram", value=False)
    if st.button("🔥 Start Scan", type="primary", width="stretch"):
        if not scan_symbols: st.error("List trống!")
        else:
            st.session_state.scan_results = None
            def process_single_symbol(sym):
                try:
                    scan_res, reason = scan_symbol(sym, days=60, ema_span=50, nav=input_nav, risk_pct=input_risk, max_positions=input_max_pos)
                    return sym, scan_res, reason
                except Exception as e: return sym, None, str(e)

            results = []; rejected = []
            progress = st.progress(0); status_txt = st.empty()
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(process_single_symbol, sym): sym for sym in scan_symbols}
                total = len(scan_symbols)
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    sym, res, reason = future.result()
                    if res: results.append(res)
                    else: rejected.append((sym, reason))
                    progress.progress((i + 1) / total)
                    status_txt.text(f"Scanning: {sym}")

            progress.empty(); status_txt.empty()
            if results:
                df_res = pd.DataFrame(results)
                df_res.sort_values(by=["Signal", "Score", "Symbol"], ascending=[True, False, True], inplace=True)
                st.session_state.scan_results = df_res
                st.success(f"Found {len(df_res)} setups!")
            else:
                st.warning("No setup found.")
            
            if auto_send_tele and st.session_state.get("scan_results") is not None:
                msg = format_scan_report(st.session_state.scan_results)
                if not msg.startswith("⚠️"): send_telegram_msg(msg)

# HIỂN THỊ KẾT QUẢ
if st.session_state.get("scan_results") is not None and not st.session_state.scan_results.empty:
    st.markdown("---")
    st.subheader("📋 Kết quả")
    
    if st.button("📒 Export Journal"):
        df_j = export_journal(st.session_state.scan_results)
        if df_j is not None: st.dataframe(df_j, hide_index=True)

    df_res = st.session_state.scan_results.copy()
    
    def format_score_ui(val):
        try: v = float(val)
        except: v = 0.0
        if v >= 4.0: return f"🔥🔥 {v}"
        if v >= 3.0: return f"⭐ {v}"
        return str(v)
    df_res["Display_Score"] = df_res["Score"].apply(format_score_ui)

    def _style_signal(val):
        if "BUY" in str(val): return "color: #22C55E; font-weight: bold"
        if "SELL" in str(val): return "color: #F87171; font-weight: bold"
        return ""

    event = st.dataframe(
        df_res.style.map(_style_signal, subset=["Signal"]),
        width="stretch", hide_index=True,
        column_order=["Symbol", "Signal", "Display_Score", "Dist_POI", "Price", "POI_D1", "KL", "SL", "TP", "Note"],
        column_config={
            "Dist_POI": st.column_config.NumberColumn("Dist%", format="%.2f%%"),
            "Price": st.column_config.NumberColumn("Price", format="%.2f"),
            "POI_D1": st.column_config.NumberColumn("POI", format="%.2f"),
            "SL": st.column_config.NumberColumn("SL", format="%.2f"),
        },
        on_select="rerun", selection_mode="single-row"
    )

    if len(event.selection.rows) > 0:
        sel_idx = event.selection.rows[0]
        st.session_state.current_symbol = df_res.iloc[sel_idx]["Symbol"]
        st.rerun()

    if st.button("📤 Gửi Tele"):
        msg = format_scan_report(st.session_state.scan_results)
        send_telegram_msg(msg)
        st.toast("Sent!")

# ==============================================================================
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