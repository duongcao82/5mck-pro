# scanner.py
import pandas as pd
import numpy as np
from datetime import datetime
from config import now_vn, TELEGRAM_KILLZONE_ONLY, TELEGRAM_ALERT_SCORE_MIN, KILLZONE_WINDOWS
from data import load_data_with_cache
from smc_core import (
    ensure_smc_columns,
    detect_entry_models,
    detect_liquidity_sweep,
    calculate_advanced_score
)
from indicators import detect_price_action


def ema(series: pd.Series, span: int = 50) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _parse_hhmm(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 60 + int(m)


def _in_killzone(now_dt, windows) -> tuple[bool, str]:
    # Ép buộc hệ thống luôn coi như đang trong giờ giao dịch (theo yêu cầu bạn)
    return True, "24/7 Mode"


# ==============================================================================
# 1. HÀM PHỤ TRỢ: KIỂM TRA NẾN (Nới lỏng điều kiện)
# ==============================================================================
def check_candlestick_signal(df, trend_side):
    if df is None or df.empty or len(df) < 6:
        return False, None, None

    # Lấy 2 dòng cuối để check tín hiệu
    rows_to_check = [df.iloc[-1]]
    if len(df) > 1:
        rows_to_check.append(df.iloc[-2])

    # Đồng bộ vol: TB 5 phiên và ngưỡng 1.2x
    vol_avg_5 = df["Volume"].tail(6).iloc[:-1].mean()

    pattern_name = None
    has_signal = False
    ref_row = df.iloc[-1]

    for row in rows_to_check:
        if row["Volume"] < vol_avg_5 * 1.2:
            continue

        if trend_side == "BUY":
            if row.get("PA_PinBar_Bull"):
                pattern_name = "Pinbar (Bull)"; has_signal = True; ref_row = row; break
            elif row.get("PA_Engulf_Bull"):
                pattern_name = "Engulfing (Bull)"; has_signal = True; ref_row = row; break
            elif row.get("PA_Fakey_Bull"):
                pattern_name = "Fakey (Bull)"; has_signal = True; ref_row = row; break

        elif trend_side == "SELL":
            if row.get("PA_PinBar_Bear"):
                pattern_name = "Pinbar (Bear)"; has_signal = True; ref_row = row; break
            elif row.get("PA_Engulf_Bear"):
                pattern_name = "Engulfing (Bear)"; has_signal = True; ref_row = row; break
            elif row.get("PA_Fakey_Bear"):
                pattern_name = "Fakey (Bear)"; has_signal = True; ref_row = row; break

    sl_price = ref_row["Low"] if trend_side == "BUY" else ref_row["High"]
    return has_signal, pattern_name, sl_price


# ==============================================================================
# 2. LOGIC CHIẾN THUẬT: SMART TIMEFRAME (AN TOÀN HƠN)
# ==============================================================================
def strategy_smart_timeframe(symbol, d1_side, df_htf=None, end_date=None):
    """
    LTF confirm (backtest-safe):
    - Load LTF theo end_date để không look-ahead
    - Confirm SMC trước, nếu chưa thì confirm PA
    - FIX lỗi NoneType * float: robust parse zone (y0/y1, low/high, bottom/top) + fallback SL
    """
    if end_date is not None:
        current_hour = end_date.hour
    else:
        current_hour = now_vn().hour

    is_confirmed = False
    final_tf = "D1"
    final_pattern = "Wait/Limit D1"
    refined_entry = None
    refined_sl = None

    df_htf = ensure_smc_columns(df_htf) if df_htf is not None and not df_htf.empty else None

    try:
        tf = "1H" if current_hour < 11 else "15m"
        days = 20 if current_hour < 11 else 5

        df_ltf_raw = load_data_with_cache(symbol, days_to_load=days, timeframe=tf, end_date=end_date)

        if df_ltf_raw is not None and not df_ltf_raw.empty:
            df_ltf = ensure_smc_columns(df_ltf_raw)

            # 1) KIỂM TRA SMC MODEL TRÊN KHUNG NHỎ
            entry_ltf = detect_entry_models(df_htf=df_htf, df_ltf=df_ltf) if df_htf is not None else None

            if entry_ltf and entry_ltf.get("entry") == d1_side:
                is_confirmed = True
                final_tf = tf
                final_pattern = f"{entry_ltf.get('model')} (HTF→LTF)"

                refined_entry = float(df_ltf_raw.iloc[-1]["Close"])

                # --- Robust zone parsing ---
                z = entry_ltf.get("zone") or {}
                lo = z.get("y0")
                if lo is None:
                    lo = z.get("low")
                if lo is None:
                    lo = z.get("bottom")

                hi = z.get("y1")
                if hi is None:
                    hi = z.get("high")
                if hi is None:
                    hi = z.get("top")

                raw_sl = lo if d1_side == "BUY" else hi

                # Fallback: nếu zone thiếu biên => dùng Low/High nến LTF hiện tại
                if raw_sl is None or (isinstance(raw_sl, float) and np.isnan(raw_sl)):
                    raw_sl = float(df_ltf_raw.iloc[-1]["Low"] if d1_side == "BUY" else df_ltf_raw.iloc[-1]["High"])
                else:
                    raw_sl = float(raw_sl)

                # Buffer 3%
                refined_sl = raw_sl * 0.97 if d1_side == "BUY" else raw_sl * 1.03

            # 2) KIỂM TRA PRICE ACTION NẾU SMC CHƯA XÁC NHẬN
            if not is_confirmed:
                df_ltf_pa = detect_price_action(df_ltf)
                has_sig, name, sl_pa = check_candlestick_signal(df_ltf_pa, d1_side)

                if has_sig:
                    is_confirmed = True
                    final_tf = tf
                    final_pattern = f"{name} (Sniper)"
                    refined_entry = float(df_ltf_raw.iloc[-1]["Close"])

                    # Fallback nếu sl_pa None
                    if sl_pa is None or (isinstance(sl_pa, float) and np.isnan(sl_pa)):
                        sl_pa = float(df_ltf_raw.iloc[-1]["Low"] if d1_side == "BUY" else df_ltf_raw.iloc[-1]["High"])
                    else:
                        sl_pa = float(sl_pa)

                    refined_sl = sl_pa * 0.97 if d1_side == "BUY" else sl_pa * 1.03

    except Exception as e:
        print(f"⚠️ Lỗi check LTF {symbol}: {e}")

    return is_confirmed, final_tf, final_pattern, refined_entry, refined_sl


# ==============================================================================
# 3. HÀM SCANNER CHÍNH (THÊM EMA50 FILTER ĐỒNG BỘ BACKTEST)
# ==============================================================================
def scan_symbol(symbol, days=200, ema_span=50, nav=1e9, risk_pct=0.01, max_positions=5):
    def _ret(res, reason):
        return (res, reason)

    try:
        # 1) Tải D1
        df_d1 = load_data_with_cache(symbol, days_to_load=days, timeframe="1D")
        if df_d1 is None or len(df_d1) < 60:
            return _ret(None, "Dữ liệu thiếu")

        df_d1 = ensure_smc_columns(df_d1)
        last_row = df_d1.iloc[-1]
        close = float(last_row["Close"])

        # 2) Lọc thanh khoản
        vol_avg_5 = df_d1["Volume"].tail(5).mean()
        if close <= 10 or vol_avg_5 < 100000 or (close * vol_avg_5 < 10_000_000):
            return _ret(None, "Thanh khoản thấp")

        # 3) EMA50 Trend Filter (ĐỒNG BỘ với backtest_scanner)
        df_d1["EMA50"] = ema(df_d1["Close"], ema_span)
        ema50 = float(df_d1["EMA50"].iloc[-1])

        # 4) Nhận diện tín hiệu (ưu tiên SMC, không có mới PA)
        side, d1_pattern, zone = "NEUTRAL", "None", None

        entry = detect_entry_models(df_htf=df_d1)
        if entry and entry.get("entry") != "NEUTRAL":
            side = entry.get("entry")
            d1_pattern = f"SMC: {entry.get('model')}"
            zone = entry.get("zone")

        if side == "NEUTRAL":
            # PA D1 chỉ dùng khi không có SMC
            df_pa = detect_price_action(df_d1.copy())
            for s in ["BUY", "SELL"]:
                has_pa, name, pa_sl = check_candlestick_signal(df_pa, s)
                if has_pa:
                    side, d1_pattern = s, name
                    zone = {"y0": pa_sl, "y1": close} if s == "BUY" else {"y0": close, "y1": pa_sl}
                    break

        if side == "NEUTRAL":
            return _ret(None, "Không có tín hiệu (D1 No Setup)")

        # 5) Áp EMA50 FILTER sau khi biết side (BUY phải > EMA50, SELL phải < EMA50)
        #    Đồng bộ với logic backtest_scanner.py :contentReference[oaicite:2]{index=2}
        if side == "BUY" and close <= (ema50 * 0.97):
            return _ret(None, "Trend EMA50 (Too Weak)")
            
        if side == "SELL" and close >= (ema50 * 1.03):
            return _ret(None, "Trend EMA50 (Too Strong)")

        # 6) Xác nhận đa khung
        is_confirm, tf_conf, pat_conf, entry_ltf, sl_ltf = strategy_smart_timeframe(symbol, side, df_htf=df_d1)

        final_poi = float(entry_ltf) if is_confirm and entry_ltf is not None else close
        # POI_D1: vùng D1 (midpoint của zone) để bạn nhìn "đúng chất SMC"
        poi_d1 = None
        if zone:
            y0 = zone.get("y0", zone.get("bottom", zone.get("low")))
            y1 = zone.get("y1", zone.get("top", zone.get("high")))
            if y0 is not None and y1 is not None:
                poi_d1 = (float(y0) + float(y1)) / 2.0
        # raw SL: ưu tiên LTF nếu confirm, không thì lấy zone D1
        if is_confirm and sl_ltf is not None:
            raw_sl = float(sl_ltf)
        else:
            raw_sl = float(zone["y0"] if side == "BUY" else zone["y1"])

        # Buffer 3% cố định (đồng bộ hệ sniper live)
        final_sl = raw_sl * 0.97 if side == "BUY" else raw_sl * 1.03
        risk = abs(final_poi - final_sl)
        if risk <= 0:
            return _ret(None, "Risk invalid")

        # 7) TP levels theo SMC
        from smc_core import detect_fvg_zones, detect_order_blocks, detect_liquidity_sweep
        fvgs = detect_fvg_zones(df_d1)
        obs = detect_order_blocks(df_d1)
        sweep_data = detect_liquidity_sweep(df_d1)

        tp_levels = []
        if side == "BUY":
            targets_fvg = [z["top"] for z in fvgs if z.get("side") == "bull" and z.get("bottom") is not None and z["bottom"] > final_poi]
            if targets_fvg:
                tp_levels.append(min(targets_fvg))

            targets_ob = [z["top"] for z in obs if z.get("side") == "bear" and z.get("bottom") is not None and z["bottom"] > final_poi]
            if targets_ob:
                tp_levels.append(min(targets_ob))

            if sweep_data == "BUY_SIDE":
                tp_levels.append(float(df_d1["High"].tail(20).max()))

            tp_levels.append(final_poi + 2.5 * risk)
        else:
            targets_fvg = [z["bottom"] for z in fvgs if z.get("side") == "bear" and z.get("top") is not None and z["top"] < final_poi]
            if targets_fvg:
                tp_levels.append(max(targets_fvg))

            targets_ob = [z["bottom"] for z in obs if z.get("side") == "bull" and z.get("top") is not None and z["top"] < final_poi]
            if targets_ob:
                tp_levels.append(max(targets_ob))

            if sweep_data == "SELL_SIDE":
                tp_levels.append(float(df_d1["Low"].tail(20).min()))

            tp_levels.append(final_poi - 2.5 * risk)

        tp_final = sorted(list(set([float(x) for x in tp_levels if x is not None])), reverse=(side == "SELL"))
        tp_str = " | ".join([f"{p:,.2f}" for p in tp_final[:3]])

        # 8) BE + Score
        final_be = final_poi + 1.8 * risk if side == "BUY" else final_poi - 1.8 * risk

        scoring_data = {
            "model_name": d1_pattern,
            "candle_pattern": pat_conf if is_confirm else d1_pattern,
            "vol_spike": last_row["Volume"] > df_d1["Volume"].tail(6).iloc[:-1].mean() * 1.2,
            "liquidity_sweep": sweep_data is not None
        }
        score_res = calculate_advanced_score(scoring_data)
        final_score = score_res["raw_score"]
        if is_confirm:
            final_score = min(final_score + 1.2, 5.0)
        
        import math

# --- QUẢN TRỊ RỦI RO SMC LINH HOẠT ---
        max_capital_per_pos = nav / max_positions
        risk_amount = nav * risk_pct
        
        # 1. Tính % khoảng cách từ giá hiện tại đến POI_D1
        dist_poi_pct = 0.0
        if poi_d1 is not None and close > 0:
            dist_poi_pct = ((close - poi_d1) / poi_d1) * 100

        # 2. Tính khối lượng (KL) theo quản trị rủi ro chuyên nghiệp 
        kl = 0
        try:
            entry_price = final_poi  
            price_diff = abs(entry_price - final_sl)
            
            if price_diff > 0:
                vol_by_risk = risk_amount / (price_diff * 1000)
                vol_by_capital = max_capital_per_pos / (entry_price * 1000)
                kl = int(min(vol_by_risk, vol_by_capital) / 100) * 100
        except:
            kl = 0

        # 3. Trả về kết quả (Phải nằm trong khối try chính) [cite: 213, 219]
        return _ret({
            "Symbol": symbol,
            "Signal": side,
            "Score": round(float(final_score), 1),
            "Price": round(close, 2),
            "POI_D1": round(float(poi_d1), 2) if poi_d1 is not None else None,
            "Dist_POI": round(dist_poi_pct, 2),
            "KL": kl,
            "ENTRY": round(float(final_poi), 2),
            "SL": round(float(final_sl), 2),
            "BE": round(float(final_be), 2),
            "TP": tp_str,
            "Note": f"{d1_pattern} | {dist_poi_pct:.1f}% from POI | {'✅ Confirm '+tf_conf if is_confirm else '⏳ D1 Only'}"
        }, "OK")

    except Exception as e:
        return _ret(None, str(e))


# =========================
# HEALTHCHECK SMC/ICT CORE
# =========================
def _healthcheck_core():
    import inspect
    import smc_core

    problems = []

    required = [
        "ensure_smc_columns",
        "detect_entry_models",
        "detect_liquidity_sweep",
        "compute_smc_levels",       # bạn đang có hàm này
        "detect_breaker_blocks",
        "entry_breaker_retest",
        "entry_ote_pullback",
    ]

    for name in required:
        if not hasattr(smc_core, name):
            problems.append(f"❌ Missing function: smc_core.{name}")

    try:
        src = inspect.getsource(smc_core)

        def _count_def(fn):
            return src.count(f"def {fn}(")

        for fn in ["detect_breaker_blocks", "entry_breaker_retest"]:
            c = _count_def(fn)
            if c > 1:
                problems.append(f"⚠️ Duplicate def detected: {fn} appears {c} times (Python will use the LAST one).")
    except Exception as e:
        problems.append(f"⚠️ Could not inspect smc_core source: {e}")

    try:
        dummy = pd.DataFrame({
            "Open": [1, 2, 3, 4, 5, 6],
            "High": [2, 3, 4, 5, 6, 7],
            "Low": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
            "Close": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
            "Volume": [100000] * 6
        })
        dummy = smc_core.ensure_smc_columns(dummy)
        br = smc_core.detect_breaker_blocks(dummy)
        if not isinstance(br, list):
            problems.append(f"❌ detect_breaker_blocks should return list, got {type(br)}")
    except Exception as e:
        problems.append(f"⚠️ Breaker return-type test failed: {e}")

    if problems:
        print("\n================ CORE HEALTHCHECK (FAIL) ================ ")
        for p in problems:
            print(p)
        print("========================================================\n")
    else:
        print("\n================ CORE HEALTHCHECK (OK) ================= ")
        print("All required functions found. No obvious duplicates. Return types OK.")
        print("========================================================\n")


# Run once on import
_healthcheck_core()


def format_scan_report(df_results):
    try:
        now = now_vn()
        is_kz, kz_name = _in_killzone(now, KILLZONE_WINDOWS)

        if df_results is None or len(df_results) == 0:
            return "⚠️ Không có mã nào đạt điểm Sniper hôm nay."

        df_results = df_results[df_results["Score"] > 2.0].copy()
        df_results = df_results[df_results["Score"] >= float(TELEGRAM_ALERT_SCORE_MIN)].copy()

        if df_results.empty:
            return f"⚠️ Không có mã nào đạt Score ≥ {TELEGRAM_ALERT_SCORE_MIN}."

        msg = "🚀 <b>SMC PRO SIGNALS</b>\n"
        msg += f"🕒 {now.strftime('%d/%m %H:%M')}\n"
        msg += "----------------\n"

        buys = df_results[df_results["Signal"] == "BUY"].sort_values(by="Score", ascending=False)
        sells = df_results[df_results["Signal"] == "SELL"].sort_values(by="Score", ascending=False)

        def make_line(idx, row):
            score = row["Score"]
            if score >= 4.0:
                icon = "🔥🔥🔥"
            elif score >= 3.0:
                icon = "⭐⭐"
            elif score > 2.0:
                icon = "🚀"
            else:
                icon = ""

            display_sym = f"<b>{row['Symbol']}</b> ({icon})" if icon else f"<b>{row['Symbol']}</b>"
            tp_str = str(row["TP"]).replace(" - ", " | ")

            # Ưu tiên POI_D1 (vùng D1), nếu không có thì fallback ENTRY/POI
            poi_val = None
            if "POI_D1" in row and pd.notna(row["POI_D1"]):
                poi_val = float(row["POI_D1"])
            elif "ENTRY" in row and pd.notna(row["ENTRY"]):
                poi_val = float(row["ENTRY"])
            elif "POI" in row and pd.notna(row["POI"]):
                poi_val = float(row["POI"])

            poi_txt = f"{poi_val:,.2f}" if poi_val is not None else "NA"

            return (f"{idx}. {display_sym} <code>{poi_txt}</code>\n"
                    f"    🛡 SL: {row['SL']:,.2f} 🎯 TP: {tp_str}\n")

        if not buys.empty:
            msg += "🟢 <b>BUY SETUP:</b>\n"
            for i, (_, row) in enumerate(buys.iterrows(), 1):
                msg += make_line(i, row)

        if not sells.empty:
            msg += "\n🔴 <b>SELL SETUP:</b>\n"
            for i, (_, row) in enumerate(sells.iterrows(), 1):
                msg += make_line(i, row)

        msg += "----------------"
        return msg

    except Exception as e:
        import traceback
        return f"❌ Lỗi tạo báo cáo: {str(e)}\n{traceback.format_exc()}"


# ==============================================================================
# 4. BÁO CÁO VNINDEX CHUYÊN SÂU (GIỮ NGUYÊN)
# ==============================================================================
def build_vnindex_report(df_vni=None):
    from smc_core import ensure_smc_columns, detect_fvg_zones, detect_order_blocks, detect_confluence_zones
    import pandas_ta as ta

    def get_zone_coords(z):
        y0 = z.get("y0", z.get("bottom"))
        y1 = z.get("y1", z.get("top"))
        # Guard: tránh None làm min/max crash
        if y0 is None or y1 is None:
            return None, None
        return min(y0, y1), max(y0, y1)

    try:
        df_vni = df_vni if df_vni is not None else load_data_with_cache("VNINDEX", 365)
        if df_vni is None or df_vni.empty:
            return "Không có dữ liệu VNINDEX!"

        df_vni.ta.sma(length=20, append=True)
        df_vni.ta.ema(length=50, append=True)
        df_vni.ta.macd(fast=12, slow=26, signal=9, append=True)
        df_vni.ta.rsi(length=14, append=True)

        last = df_vni.iloc[-1]
        close = last["Close"]
        ma20 = last.get("SMA_20", 0)
        ema50 = last.get("EMA_50", 0)
        rsi = last.get("RSI_14", 50)

        macd_h = last.get("MACDh_12_26_9", 0)
        macd_signal = "🟢 Cắt lên" if macd_h > 0 else "🔴 Cắt xuống"

        vol_now = last["Volume"]
        vol_avg_5 = df_vni["Volume"].tail(5).mean()
        vol_status = "🔥 Đột biến" if vol_now > vol_avg_5 * 1.2 else "⚖️ Ổn định" if vol_now > vol_avg_5 * 0.7 else "💤 Cạn cung"

        # Trend chính theo EMA50 (đồng bộ với logic lọc BUY/SELL của hệ thống)
        trend_icon = "🟢" if close >= ema50 else "🔴"
        trend_text = "UPTREND (EMA50)" if close >= ema50 else "DOWNTREND (EMA50)"

        # Sideway khi giá quanh EMA50 (±1%)
        band = 0.01
        if ema50 and abs(close - ema50) / ema50 <= band:
            trend_icon = "🟡"
            trend_text = "SIDEWAY (near EMA50)"

        # Momentum/nhịp theo SMA20 (phụ)
        momentum = "🔼 Strong" if close >= ma20 else "🔽 Weak"

        df_vni = ensure_smc_columns(df_vni)
        fvg_zones = detect_fvg_zones(df_vni, max_zones=10)
        obs = detect_order_blocks(df_vni)
        strong_fvgs, strong_ob = detect_confluence_zones(df_vni, fvg_zones, obs[-1] if obs else None)

        sup_zone = None
        for z in strong_fvgs + ([strong_ob] if strong_ob else []):
            if not z:
                continue
            l, h = get_zone_coords(z)
            if l is None or h is None:
                continue
            if h < close * 0.999:
                if sup_zone is None or h > get_zone_coords(sup_zone)[1]:
                    sup_zone = z
        sup_price = get_zone_coords(sup_zone)[1] if sup_zone else df_vni["Low"].tail(30).min()

        res_zone = None
        for z in strong_fvgs + ([strong_ob] if strong_ob else []):
            if not z:
                continue
            l, h = get_zone_coords(z)
            if l is None or h is None:
                continue
            if l > close * 1.001:
                if res_zone is None or l < get_zone_coords(res_zone)[0]:
                    res_zone = z
        res_price = get_zone_coords(res_zone)[0] if res_zone else df_vni["High"].tail(30).max()

        bull_scenario = f"""📈 <b>KỊCH BẢN TĂNG (BULLISH):</b>
   • Điều kiện: Giá vượt dứt khoát mốc kháng cự <code>{res_price:,.0f}</code> với Vol lớn.
   • Hành động: Mua gia tăng khi có nhịp Retest thành công.
   • Target ngắn hạn: <code>{res_price * 1.05:,.0f}</code>."""

        bear_scenario = f"""📉 <b>KỊCH BẢN GIẢM (BEARISH):</b>
   • Điều kiện: Giá thủng hỗ trợ cứng <code>{sup_price:,.0f}</code>.
   • Hành động: Hạ tỷ trọng Margin, quản trị rủi ro triệt để.
   • Vùng cân bằng dự kiến: <code>{sup_price * 0.95:,.0f}</code>."""

        sideway_scenario = f"""🟡 <b>KỊCH BẢN ĐI NGANG (SIDEWAY):</b>
   • Biên độ: <code>{sup_price:,.0f}</code> - <code>{res_price:,.0f}</code>.
   • Chiến lược: "Buy Low - Sell High".
   • Mua tại biên dưới ({sup_price:,.0f}), Bán tại biên trên ({res_price:,.0f})."""

        msg = f"""
<b>NHẬN ĐỊNH VNINDEX | 📅 {now_vn().strftime('%d/%m/%Y')}</b>
------------------------------------
<b>1️⃣ CẤU TRÚC THỊ TRƯỜNG</b>
📍 Close: <b>{close:,.2f}</b>
🚀 Xu hướng: {trend_icon} {trend_text} | Nhịp: {momentum}
🛡 Hỗ trợ cứng: <code>{sup_price:,.2f}</code>
⚔️ Kháng cự: <code>{res_price:,.2f}</code>
🌊 RSI: {rsi:.1f} | 📊 Vol: {vol_status}
------------------------------------
<b>2️⃣ KẾ HOẠCH HÀNH ĐỘNG (ACTION PLAN)</b>

{bull_scenario}

{sideway_scenario}

{bear_scenario}
------------------------------------
<i>⚠️ Lưu ý: Quản trị rủi ro là ưu tiên hàng đầu.</i>
"""
        return msg
    except Exception as e:
        return f"Lỗi tạo báo cáo: {str(e)}"


def process_and_send_scan_report(df_results):
    try:
        from telegram_bot import send_telegram_msg
        msg = format_scan_report(df_results)
        if msg.startswith("⏳") or msg.startswith("⚠️"):
            return False, msg
        ok = send_telegram_msg(msg)
        return ok, "Đã gửi tín hiệu scan!" if ok else "Gửi thất bại"
    except Exception as e:
        return False, f"Lỗi gửi Telegram: {e}"


def process_and_send_vnindex_report():
    try:
        msg = build_vnindex_report()
        from telegram_bot import send_telegram_msg
        ok = send_telegram_msg(msg)
        if ok:
            return True, "Gửi báo cáo VNINDEX thành công"
        else:
            return False, "Gửi tin nhắn Telegram thất bại (Kiểm tra Token/ChatID)"
    except Exception as e:
        return False, f"Lỗi gửi báo cáo: {str(e)}"

def export_journal(df_results):
    """
    Chuyển kết quả scanner -> Trading Journal (Google Sheets)
    - Chỉ giữ 3 model CORE
    - Chỉ giữ RR >= 1:2
    - KHÔNG tự ghi Result / Emotion / R
    """
    if df_results is None or df_results.empty:
        return None

    rows = []
    today = now_vn().strftime("%Y-%m-%d")

    CORE_MODELS = [
        "SMC: Silver Bullet",
        "SMC: OTE Pullback",
        "SMC: Breaker Block Retest"
    ]

    for _, r in df_results.iterrows():
        # --------- MODEL FILTER ----------
        note = str(r.get("Note", ""))
        model_name = note.split("|")[0].strip()

        if not any(m in model_name for m in CORE_MODELS):
            continue

        # --------- ENTRY / SL / TP ----------
        entry = r.get("ENTRY", r.get("Price"))
        sl = r.get("SL")

        tp = None
        if isinstance(r.get("TP"), str):
            try:
                tp = float(r["TP"].split("|")[0].replace(",", "").strip())
            except Exception:
                tp = None

        # --------- RR FILTER ----------
        rr_plan = None
        try:
            if entry and sl and tp:
                rr_plan = round(abs(tp - entry) / abs(entry - sl), 2)
        except Exception:
            rr_plan = None

        if rr_plan is None or rr_plan < 2.0:
            continue

        # --------- EMA50 STATUS ----------
        ema_status = ""
        if "EMA50" in note:
            ema_status = "Above" if r.get("Signal") == "BUY" else "Below"

        # --------- BUILD JOURNAL ROW ----------
        rows.append({
            "Date": today,
            "Symbol": r.get("Symbol"),
            "Side": r.get("Signal"),
            "Model": model_name,
            "EMA50": ema_status,
            "HTF_POI": r.get("POI_D1"),
            "Price": entry,
            "KL": r.get("KL"),  
            "SL": sl,
            "TP": tp,
            "Risk_%": "",                 # trader tự điền
            "RR_Plan": rr_plan,
            "LTF_Confirm": "Yes" if "Confirm" in note else "No",
            "Note": note
        })

    if not rows:
        return None

    return pd.DataFrame(rows)
