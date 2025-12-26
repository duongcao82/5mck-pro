import os
import sys
import time
import concurrent.futures
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

pd.set_option("future.no_silent_downcasting", True)

# Path Fix (giữ như bạn đang làm)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# --- IMPORT MODULES ---
from universe import get_vnallshare_universe
from data import load_data_with_cache, load_smart_money_data
from viz import plot_single_timeframe, plot_smart_money
from smc_core import (
    ensure_smc_columns,
    compute_smc_levels,
    detect_fvg_zones,
    detect_order_blocks,
    detect_trendlines,
    detect_confluence_zones,
)
from scanner import scan_symbol, process_and_send_vnindex_report
from indicators import detect_rsi_divergence
from pipeline_manager import run_bulk_update


# ==============================================================================
# Helpers
# ==============================================================================
def core_healthcheck_ui():
    import smc_core
    issues = []
    must = [
        "compute_smc_levels",
        "entry_breaker_retest",
        "detect_breaker_blocks",
        "detect_entry_models",
    ]
    for n in must:
        if not hasattr(smc_core, n):
            issues.append(f"Missing smc_core.{n}")
    return issues


@st.cache_data(ttl=600, show_spinner=False)
def load_smart_money_cached(symbol: str):
    """Cache Smart Money tối đa 10 phút để tránh gọi API liên tục."""
    return load_smart_money_data(symbol)


def plotly_draw_config():
    return {
        "scrollZoom": True,
        "displayModeBar": True,
        "modeBarButtonsToAdd": ["drawline", "drawopenpath", "drawcircle", "drawrect", "eraseshape"],
        "modeBarButtonsToRemove": ["lasso2d", "select2d"],
        "displaylogo": False,
    }


# ==============================================================================
# 1. SETUP UI & CONFIG
# ==============================================================================
st.set_page_config(page_title="5MCK Pro", layout="wide", page_icon="📈")

st.caption("Giờ Việt Nam: " + datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d %H:%M:%S"))

st.markdown(
    """
    <style>
        .stApp { background-color: #0e1117; color: white; } 
        .metric-card { background-color: #262730; padding: 10px; border-radius: 5px; margin-bottom: 10px; }
        .stTabs [data-baseweb="tab-list"] { gap: 24px; }
        .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: #0e1117; border-radius: 4px 4px 0px 0px; gap: 1px; padding-top: 10px; padding-bottom: 10px; }
        .stTabs [aria-selected="true"] { background-color: #262730; color: white; border-top: 2px solid #00e676; }
    </style>
    """,
    unsafe_allow_html=True,
)

if "current_symbol" not in st.session_state:
    st.session_state.current_symbol = "VNINDEX"

# ==============================================================================
# 2. SIDEBAR CONTROL
# ==============================================================================
st.sidebar.empty()

current_dir = os.path.dirname(os.path.abspath(__file__))
img_logo = os.path.join(current_dir, "5MCK_Logo.png")
img_bidv = os.path.join(current_dir, "5MCK_BIDV.png")
img_vps = os.path.join(current_dir, "5MCK_VPS.jpg")

if os.path.exists(img_logo):
    try:
        st.sidebar.image(img_logo, width="stretch")
    except Exception:
        st.sidebar.image(img_logo, width="stretch")
else:
    st.sidebar.title("🎛️ 5MCK Control")

st.sidebar.write("")

col_logo1, col_logo2 = st.sidebar.columns(2)
with col_logo1:
    if os.path.exists(img_bidv):
        try:
            st.sidebar.image(img_bidv, width="stretch")
        except Exception:
            st.sidebar.image(img_bidv, width="stretch")
with col_logo2:
    if os.path.exists(img_vps):
        try:
            st.sidebar.image(img_vps, width="stretch")
        except Exception:
            st.sidebar.image(img_vps, width="stretch")

st.sidebar.markdown("---")
use_smart_money = st.sidebar.checkbox("💰 Smart Money (Foreign/Prop/Depth)", value=False)

symbol_input = st.sidebar.text_input("🔍 Tra cứu Mã:", value=st.session_state.current_symbol).upper()
if symbol_input != st.session_state.current_symbol:
    st.session_state.current_symbol = symbol_input
    st.rerun()

st.sidebar.markdown("---")
if st.sidebar.button("📢 BC VNINDEX"):
    with st.spinner("Đang phân tích..."):
        success, msg = process_and_send_vnindex_report()
        if success:
            st.sidebar.success("Đã gửi báo cáo!")
        else:
            st.sidebar.error(f"Lỗi: {msg}")

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


# ==============================================================================
# PROCESS: build fig + zones (PA2)
# ==============================================================================
def process_and_plot(
    df,
    interval,
    show_vol_param=True,
    show_ma_param=True,
    show_vsa_param=False,
    htf_zones=None,
    skip_current_zones=False,
    enable_smart_money=False,
    build_fig=True,
):
    """
    PA2: Hàm chỉ xử lý và TRẢ VỀ (fig, zones). Không vẽ trong hàm.
    - build_fig=False: chỉ tính zones để làm HTF nền (nhanh hơn rất nhiều).
    """
    if htf_zones is None:
        htf_zones = []

    if df is None or df.empty:
        return None, []

    current_sym = st.session_state.current_symbol
    df = ensure_smc_columns(df)

    # 1) Smart money: chỉ load khi bật
    df_smart_money = None
    if enable_smart_money:
        try:
            res_sm = load_smart_money_cached(current_sym)
            # data.py trả tuple (foreign, prop, depth)
            # viz.py của bạn đang dùng smart_money_data, nên ta truyền nguyên tuple hoặc df_depth tùy viz
            df_smart_money = res_sm
        except Exception:
            df_smart_money = None

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

    # 3) Nếu chỉ cần zones HTF thì khỏi dựng Plotly fig (rất nhanh)
    if not build_fig:
        return None, zones_out

    fig = plot_single_timeframe(
        df,
        current_sym,
        interval,
        smc_levels=smc,
        fvg_zones=plot_fvgs,
        ob_zones=plot_obs,
        htf_zones=htf_zones,
        trendlines=t_lines,
        rsi_divergences=rsi_divs,
        show_vol=show_vol_param,
        show_ma=(show_ma_param and use_ma),
        show_vsa=show_vsa_param,
        smart_money_data=df_smart_money,
        show_rsi=use_rsi,
        show_smc=use_smc,
    )

    return fig, zones_out


# ==============================================================================
# 4. MAIN DASHBOARD (ALL-IN-ONE)
# ==============================================================================
st.title(f"📊 Phân tích Kỹ thuật: {st.session_state.current_symbol}")

symbol = st.session_state.current_symbol
df_1d = load_data_with_cache(symbol, days_to_load=365, timeframe="1D")

if df_1d is not None and not df_1d.empty:
    last = df_1d.iloc[-1]
    prev = df_1d.iloc[-2] if len(df_1d) > 1 else last
    chg = last["Close"] - prev["Close"]
    pct = (chg / prev["Close"]) * 100 if prev["Close"] != 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Giá đóng cửa", f"{last['Close']:,.2f}", f"{chg:,.2f} ({pct:.2f}%)")
    c2.metric("Khối lượng (Vol)", f"{last['Volume']:,.0f}")
    c3.metric("RSI (14)", f"{last.get('RSI_14', 0):.2f}")

    ma20 = last.get("SMA_20", 0)
    ema50 = last.get("EMA_50", 0)
    trend = "UP 🚀" if last["Close"] > ma20 else "DOWN 🐻"
    if last["Close"] > ma20 and last["Close"] < ema50:
        trend = "SIDEWAY 🦀"
    c4.metric("Trend", trend)

    tf_choice = st.radio(
        "Chọn khung thời gian",
        ["📅 Daily (1D)", "⚡ Hourly (1H)", "⏱️ 15 Minutes"],
        horizontal=True,
    )

    # cache zones trong session để giảm tính toán lại
    if "d1_zones" not in st.session_state:
        st.session_state.d1_zones = []
    if "h1_zones" not in st.session_state:
        st.session_state.h1_zones = []

    # ========= 1D =========
    if tf_choice == "📅 Daily (1D)":
        with st.spinner("Đang dựng biểu đồ 1D..."):
            fig_d1, d1_zones = process_and_plot(
                df_1d,
                "1D",
                show_vol_param=use_vol,
                show_ma_param=use_ma,
                show_vsa_param=use_vsa,
                htf_zones=[],
                skip_current_zones=False,
                enable_smart_money=use_smart_money,
                build_fig=True,
            )
            st.session_state.d1_zones = d1_zones

        if fig_d1 is not None:
            st.plotly_chart(fig_d1, use_container_width=True, config=plotly_draw_config())

    # ========= 1H =========
    elif tf_choice == "⚡ Hourly (1H)":
        # đảm bảo có HTF D1 zones, nhưng không cần dựng fig D1
        if not st.session_state.d1_zones:
            _, st.session_state.d1_zones = process_and_plot(
                df_1d,
                "1D",
                show_vol_param=False,
                show_ma_param=False,
                show_vsa_param=False,
                htf_zones=[],
                enable_smart_money=False,
                build_fig=False,  # chỉ lấy zones
            )

        df_1h = load_data_with_cache(symbol, days_to_load=200, timeframe="1H")

        if df_1h is not None and not df_1h.empty:
            with st.spinner("Đang dựng biểu đồ 1H..."):
                fig_h1, h1_zones = process_and_plot(
                    df_1h,
                    "1H",
                    show_vol_param=False,
                    show_ma_param=False,
                    show_vsa_param=False,
                    htf_zones=st.session_state.d1_zones,
                    skip_current_zones=False,
                    enable_smart_money=use_smart_money,
                    build_fig=True,
                )
                st.session_state.h1_zones = h1_zones

            if fig_h1 is not None:
                st.plotly_chart(fig_h1, use_container_width=True, config=plotly_draw_config())
        else:
            st.info("⏳ Đang tải dữ liệu 1H...")

    # ========= 15m =========
    else:
        # đảm bảo D1 zones (không dựng fig)
        if not st.session_state.d1_zones:
            _, st.session_state.d1_zones = process_and_plot(
                df_1d,
                "1D",
                show_vol_param=False,
                show_ma_param=False,
                show_vsa_param=False,
                htf_zones=[],
                enable_smart_money=False,
                build_fig=False,
            )

        use_h1_overlay = st.checkbox("Overlay zones 1H lên 15m", value=False)

        h1_zones = []
        if use_h1_overlay:
            # load H1 zones (không dựng fig) để overlay lên 15m
            df_1h = load_data_with_cache(symbol, days_to_load=200, timeframe="1H")
            if df_1h is not None and not df_1h.empty:
                _, h1_zones = process_and_plot(
                    df_1h,
                    "1H",
                    show_vol_param=False,
                    show_ma_param=False,
                    show_vsa_param=False,
                    htf_zones=st.session_state.d1_zones,
                    enable_smart_money=False,  # tránh gọi smart money thêm lần
                    build_fig=False,
                )
                for z in h1_zones:
                    z["is_from_1h"] = True

        df_15m = load_data_with_cache(symbol, days_to_load=400, timeframe="15m")

        if df_15m is not None and not df_15m.empty:
            final_htf = list(st.session_state.d1_zones) + list(h1_zones)
            with st.spinner("Đang dựng biểu đồ 15m..."):
                fig_15, _ = process_and_plot(
                    df_15m,
                    "15m",
                    show_vol_param=False,
                    show_ma_param=False,
                    show_vsa_param=False,
                    htf_zones=final_htf,
                    skip_current_zones=True,
                    enable_smart_money=use_smart_money,
                    build_fig=True,
                )

            if fig_15 is not None:
                st.plotly_chart(fig_15, use_container_width=True, config=plotly_draw_config())
        else:
            st.info("⏳ Đang tải dữ liệu 15m...")

else:
    st.error(f"⚠️ Chưa có dữ liệu {symbol}. Hãy bấm 'Cập nhật Dữ liệu' bên dưới.")

# ==============================================================================
# --- [PHẦN 3] SCANNER & PIPELINE (ĐỘC LẬP) ---
# ==============================================================================
st.markdown("---")
st.subheader("🚀 SMC Scanner")

default_str = """ACB, AGR, ANV, BAF, BCM, BID, BMP, BSI, BVH, CII, CTD, CTG, CTR, CTS, DBC, DCL, DCM, DGC, DGW, DHA, DIG, DPG, DPM, DXG, EIB, ELC, EVF, FCN, FPT, FRT, FTS, GAS, GEX, GMD, GVR, HAG, HAH, HCM, HDB, HDC, HDG, HHS, HHV, HPG, HSG, IJC, KBC, KDC, KDH, KOS, KSB, LCG, LPB, MBB, MSB, MSN, MWG, NAB, NAF, NKG, NLG, NT2, NTL, OCB, ORS, PAN, PC1, PDR, PET, PHR, PLX, PNJ, POW, PVD, PVT, QCG, REE, SAB, SBT, SCS, SHB, SHI, SIP, SSB, SSI, STB, TCB, TCH, TCM, TLG, TPB, VCB, VCG, VCI, VDS, VGC, VHC, VHM, VIB, VIC, VIX, VJC, VND, VNM, VPB, VPI, VRE, VSC, VTP, YEG"""

if "scan_symbols_text" not in st.session_state:
    st.session_state.scan_symbols_text = default_str

col_u1, col_u2 = st.columns([1, 3])
with col_u1:
    if st.button("🌍 Load VN-Universe"):
        with st.spinner("Đang lọc dữ liệu Universe (cache check)..."):
            try:
                uni_list = get_vnallshare_universe(days=20)
                if uni_list:
                    st.session_state.scan_symbols_text = ", ".join(uni_list)
                    st.success(f"Đã load {len(uni_list)} mã từ Universe!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.warning("Không tìm thấy mã nào thỏa mãn điều kiện Universe.")
            except Exception as e:
                st.error(f"Lỗi Load Universe: {e}")

with col_u2:
    st.info("Bấm nút bên trái để lấy danh sách mã lọc tự động theo thanh khoản.")

scan_list_input = st.text_area(
    "Danh sách mã (Tự động điền hoặc nhập tay):",
    value=st.session_state.scan_symbols_text,
    height=100,
)
if scan_list_input != st.session_state.scan_symbols_text:
    st.session_state.scan_symbols_text = scan_list_input

raw_symbols = scan_list_input.replace("\n", " ").replace(",", " ").replace(";", " ")
scan_symbols = [s.strip().upper() for s in raw_symbols.split(" ") if s.strip()]
st.caption(f"✅ Đã nhận diện: **{len(scan_symbols)}** mã sẵn sàng để Scan.")

c_btn1, c_btn2 = st.columns(2)

issues = core_healthcheck_ui()
if issues:
    st.error("CORE HEALTHCHECK FAIL:\n" + "\n".join([f"- {x}" for x in issues]))
    st.stop()
else:
    st.success("CORE HEALTHCHECK OK ✅")

with c_btn1:
    st.write("1️⃣ **Bước 1: Update Cache**")
    if st.button("📥 Cập nhật Dữ liệu", width="stretch"):
        if not scan_symbols:
            st.error("Danh sách trống!")
        else:
            with st.status("⏳ Đang tải dữ liệu đa luồng...", expanded=True) as status:
                res = run_bulk_update(scan_symbols, days_back=365)
                if res == "Xong":
                    status.update(label="✅ Đã cập nhật Cache!", state="complete", expanded=False)
                    st.toast("Dữ liệu đã sẵn sàng!", icon="💾")
                    time.sleep(1)
                    st.rerun()
                else:
                    status.update(label="❌ Có lỗi xảy ra", state="error")
                    st.error(res)

with c_btn2:
    st.write("2️⃣ **Bước 2: Tìm cơ hội**")
    auto_send_tele = st.checkbox(
        "✅ Auto gửi Telegram sau khi scan",
        value=False,
        help="Tự động gửi tín hiệu Telegram nếu đủ Score và đúng Killzone",
    )

    if st.button("🔥 Start Scan", type="primary", width="stretch"):
        if not scan_symbols:
            st.error("Danh sách trống!")
        else:
            st.session_state.scan_results = None

            def process_single_symbol(sym):
                try:
                    scan_res, reason = scan_symbol(
                        sym,
                        days=60,
                        ema_span=50,
                        nav=input_nav,
                        risk_pct=input_risk,
                        max_positions=input_max_pos,
                    )
                    return sym, scan_res, reason
                except Exception as e:
                    return sym, None, str(e)

            results = []
            rejected = []
            progress = st.progress(0)
            status_txt = st.empty()

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(process_single_symbol, sym): sym for sym in scan_symbols}
                total = len(scan_symbols)

                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    sym, res, reason = future.result()
                    if res:
                        results.append(res)
                    else:
                        rejected.append((sym, reason))
                    progress.progress((i + 1) / total)
                    status_txt.text(f"Đang quét: {sym} ({i+1}/{total})")

            progress.empty()
            status_txt.empty()

            if rejected:
                df_rej = pd.DataFrame(rejected, columns=["Symbol", "Reason"])
                st.info(f"📌 Quét xong {total} mã: ✅ {len(results)} đạt tín hiệu | ❌ {len(rejected)} bị loại.")

                summary = df_rej["Reason"].value_counts().reset_index()
                summary.columns = ["Reason", "Count"]

                with st.expander("📊 Xem thống kê lý do bị loại (Top)", expanded=True):
                    st.table(summary)

                with st.expander("🔍 Chi tiết từng mã bị loại"):
                    st.dataframe(df_rej, use_container_width=True, hide_index=True)

            if results:
                df_res = pd.DataFrame(results)
                df_res.sort_values(by=["Signal", "Score", "Symbol"], ascending=[True, False, True], inplace=True)
                st.session_state.scan_results = df_res
                st.success(f"Tìm thấy {len(df_res)} cơ hội!")
            else:
                st.session_state.scan_results = None
                st.warning("Không tìm thấy cơ hội phù hợp.")

            if auto_send_tele and st.session_state.get("scan_results") is not None:
                from scanner import format_scan_report
                from telegram_bot import send_telegram_msg

                msg = format_scan_report(st.session_state.scan_results)
                if msg.startswith("⏳") or msg.startswith("⚠️"):
                    st.info(msg)
                else:
                    ok = send_telegram_msg(msg)
                    if ok:
                        st.toast("✅ Đã auto gửi Telegram!", icon="🚀")
                    else:
                        st.error("❌ Gửi Telegram thất bại. Kiểm tra Token / Chat ID.")


# HIỂN THỊ KẾT QUẢ SCAN
if st.session_state.get("scan_results") is not None and not st.session_state.scan_results.empty:
    st.markdown("---")
    st.subheader("📋 Kết quả Quét Tín hiệu (SMC/ICT)")

    from scanner import export_journal

    if st.button("📒 Xuất Trading Journal"):
        df_journal = export_journal(st.session_state.scan_results)
        if df_journal is not None and not df_journal.empty:
            st.success("Đã tạo Trading Journal – copy sang Google Sheets")
            st.dataframe(df_journal, use_container_width=True, hide_index=True)
        else:
            st.warning("Không có dữ liệu để xuất Journal")

    df_res = st.session_state.scan_results.copy()

    def format_score_ui(val):
        try:
            v = float(val)
        except Exception:
            v = 0.0
        if v >= 4.0:
            return f"🔥🔥🔥 {v}"
        if v >= 3.0:
            return f"⭐⭐ {v}"
        if v > 2.0:
            return f"🚀 {v}"
        return str(v)

    df_res["Display_Score"] = df_res["Score"].apply(format_score_ui)

    def _style_signal(val):
        v = str(val).upper()
        if "BUY" in v:
            return "color: #22C55E; font-weight: 700"
        if "SELL" in v:
            return "color: #F87171; font-weight: 700"
        return ""

    def _style_dist_poi(val):
        try:
            v = float(val)
            if abs(v) > 3.0:
                return "color: #FFA500;"
            if abs(v) < 1.0:
                return "color: #00E676;"
            return ""
        except Exception:
            return ""

    column_order = ["Symbol", "Signal", "Display_Score", "Dist_POI", "Price", "POI_D1", "KL", "SL", "BE", "TP", "Note"]

    event = st.dataframe(
        df_res.style.map(_style_signal, subset=["Signal"]).map(_style_dist_poi, subset=["Dist_POI"]),
        width="stretch",
        hide_index=True,
        column_order=column_order,
        column_config={
            "Symbol": st.column_config.TextColumn("Tickers", width="small"),
            "Signal": st.column_config.TextColumn("Signal", width="small"),
            "Display_Score": st.column_config.TextColumn("Score", width="medium"),
            "Dist_POI": st.column_config.NumberColumn("Dist POI %", format="%.2f%%", help="Khoảng cách % từ giá hiện tại đến HTF POI"),
            "Price": st.column_config.NumberColumn("Price", format="%.2f"),
            "POI_D1": st.column_config.NumberColumn("HTF POI", format="%.2f"),
            "KL": st.column_config.NumberColumn("KL (cp)"),
            "SL": st.column_config.NumberColumn("SL", format="%.2f"),
            "BE": st.column_config.NumberColumn("BE", format="%.2f"),
            "TP": st.column_config.TextColumn("TP", width="medium"),
            "Note": st.column_config.TextColumn("Notes", width="large"),
        },
        on_select="rerun",
        selection_mode="single-row",
    )

    if len(event.selection.rows) > 0:
        sel_idx = event.selection.rows[0]
        sel_sym = df_res.iloc[sel_idx]["Symbol"]
        if sel_sym != st.session_state.current_symbol:
            st.session_state.current_symbol = sel_sym
            st.rerun()

    if st.button("📤 Gửi Telegram", key="btn_send_tele"):
        from scanner import format_scan_report
        from telegram_bot import send_telegram_msg

        msg = format_scan_report(st.session_state.scan_results)
        if send_telegram_msg(msg):
            st.toast("Đã gửi báo cáo lên Telegram!", icon="✅")
        else:
            st.error("Gửi thất bại. Hãy kiểm tra Token/Chat ID.")


# ==============================================================================
# [NEW] AI MINI BOT: TRA CỨU TÍN HIỆU NHANH
# ==============================================================================
st.sidebar.markdown("---")
st.sidebar.subheader("🤖 Bot 5mCK")

bot_query = st.sidebar.text_input("Tra cứu nhanh", placeholder="Nhập mã CP...", label_visibility="collapsed").upper().strip()
if bot_query:
    with st.sidebar.status(f"🕵️ Bot đang soi {bot_query}...", expanded=True) as status:
        try:
            res, reason = scan_symbol(bot_query, days=100)
            if res:
                status.update(label="✅ Đã tìm thấy cơ hội!", state="complete", expanded=True)
                icon = "🟢" if res["Signal"] == "BUY" else "🔴"
                try:
                    score_val = float(res.get("Score", 0))
                except Exception:
                    score_val = 0.0
                fire = "🔥" * int(max(0, round(score_val - 2)))

                st.sidebar.markdown(
                    f"""
                    ### {icon} {res['Signal']} **{bot_query}** {fire}
                    - **POI:** `{res.get('POI', 0):,.2f}`
                    - **SL:** `{res.get('SL', 0):,.2f}`
                    - **TP:** `{res.get('TP', '')}`
                    """
                )

                if st.sidebar.button(f"📊 Xem Chart {bot_query}", key="btn_bot_view"):
                    st.session_state.current_symbol = bot_query
                    st.rerun()
            else:
                status.update(label="zzz Chưa có kèo thơm", state="complete", expanded=True)
                st.sidebar.info(f"🤖 {bot_query}: {reason}")
        except Exception as e:
            status.update(label="❌ Lỗi", state="error")
            st.sidebar.error(f"Lỗi: {e}")
