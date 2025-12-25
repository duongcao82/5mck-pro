# backtest_scanner.py
import warnings

warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
)
import pandas as pd
import numpy as np
# Silence pandas FutureWarning: downcasting on fillna/ffill/bfill

from smc_core import (
    ensure_smc_columns,
    detect_entry_models,
    detect_fvg_zones,
    detect_order_blocks,
    detect_liquidity_sweep,
    calculate_advanced_score,
)
from indicators import detect_price_action
from scanner import strategy_smart_timeframe, check_candlestick_signal


def ema(series: pd.Series, span: int):
    return series.ewm(span=span, adjust=False).mean()


def atr(df: pd.DataFrame, period: int = 14):
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    close = df["Close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low).abs(),
         (high - prev_close).abs(),
         (low - prev_close).abs()],
        axis=1
    ).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def normalize_end_dt(dt):
    if dt is None:
        return None
    dt = pd.Timestamp(dt)
    if dt.hour == 0 and dt.minute == 0:
        return dt.replace(hour=14, minute=30)
    return dt


def scan_symbol_backtest(
    symbol: str,
    df_d1: pd.DataFrame,
    score_min=3.5,
    ema_span=50,
    atr_period=14,
    atr_k=0.8,
):
    try:
        if df_d1 is None or len(df_d1) < 60:
            return None, "Dữ liệu thiếu"

        df_d1 = ensure_smc_columns(df_d1.copy())
        last = df_d1.iloc[-1]
        close = float(last["Close"])

        vol_avg_5 = df_d1["Volume"].tail(5).mean()
        if close <= 10 or vol_avg_5 < 100000 or close * vol_avg_5 < 3_000_000:
            return None, "Thanh khoản thấp"

        df_d1["EMA50"] = ema(df_d1["Close"], ema_span)
        df_d1["ATR"] = atr(df_d1, atr_period)

        ema50 = float(df_d1["EMA50"].iloc[-1])
        atr_v = float(df_d1["ATR"].iloc[-1])

        side, model, zone = "NEUTRAL", "", None

        entry = detect_entry_models(df_htf=df_d1)
        if entry:
            side = entry["entry"]
            model = f"SMC: {entry.get('model')}"
            zone = entry.get("zone")

        if side == "NEUTRAL":
            df_pa = detect_price_action(df_d1.copy())
            for s in ["BUY", "SELL"]:
                ok, name, pa_sl = check_candlestick_signal(df_pa, s)
                if ok:
                    side = s
                    model = name
                    zone = {"y0": pa_sl, "y1": close}
                    break

        if side == "NEUTRAL":
            return None, "No setup"

        if side == "BUY" and close <= ema50:
            return None, "Trend EMA50"
        if side == "SELL" and close >= ema50:
            return None, "Trend EMA50"

        end_dt = normalize_end_dt(df_d1.index[-1])
        is_ok, tf, pat, entry_ltf, sl_ltf = strategy_smart_timeframe(
            symbol, side, df_htf=df_d1, end_date=end_dt
        )

        poi = float(entry_ltf) if is_ok and entry_ltf else close
        raw_sl = float(sl_ltf) if is_ok and sl_ltf else (
            zone["y0"] if side == "BUY" else zone["y1"]
        )

        if side == "BUY":
            sl = raw_sl - atr_k * atr_v
        else:
            sl = raw_sl + atr_k * atr_v

        risk = abs(poi - sl)
        if risk <= 0:
            return None, "Risk invalid"

        fvgs = detect_fvg_zones(df_d1)
        obs = detect_order_blocks(df_d1)
        sweep = detect_liquidity_sweep(df_d1)

        tps = []
        if side == "BUY":
            tps += [z["top"] for z in fvgs if z["side"] == "bull" and z["y0"] > poi]
            tps += [z["top"] for z in obs if z["side"] == "bear" and z["y0"] > poi]
            if sweep == "BUY_SIDE":
                tps.append(df_d1["High"].tail(20).max())
            tps.append(poi + 2.5 * risk)
        else:
            tps += [z["bottom"] for z in fvgs if z["side"] == "bear" and z["y1"] < poi]
            tps += [z["bottom"] for z in obs if z["side"] == "bull" and z["y1"] < poi]
            if sweep == "SELL_SIDE":
                tps.append(df_d1["Low"].tail(20).min())
            tps.append(poi - 2.5 * risk)

        tp = sorted(set(tps), reverse=(side == "SELL"))[0]

        score = calculate_advanced_score({
            "model_name": model,
            "vol_spike": last["Volume"] > df_d1["Volume"].tail(6).iloc[:-1].mean() * 1.2,
            "liquidity_sweep": sweep is not None
        })["raw_score"]

        if is_ok:
            score += 1.2

        if score < score_min:
            return None, "Score low"

        return {
            "Symbol": symbol,
            "Signal": side,
            "Score": round(score, 1),
            "POI": round(poi, 2),
            "SL": round(sl, 2),
            "TP": round(tp, 2),
            "Model": model,
            "ConfirmTF": tf if is_ok else "D1",
            "IsConfirm": is_ok,
            "ATR": round(atr_v, 4),
            "EMA50": round(ema50, 2),
        }, "OK"

    except Exception as e:
        return None, str(e)
