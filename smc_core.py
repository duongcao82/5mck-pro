# src/smc_core.py
import pandas as pd
import numpy as np

# ==============================================================================
# 0) SWINGS (Fractals) + ENSURE COLUMNS
# ==============================================================================

def detect_swings(df: pd.DataFrame, lookback: int = 2, copy: bool = True) -> pd.DataFrame:
    """
    Fractals kiểu 5 nến (lookback=2).
    """
    if df is None or df.empty or len(df) < (lookback * 2 + 1):
        return df

    if copy:
        df = df.copy()
    # Ensure columns exist (in-place when copy=False)
    df["Swing_High"] = False
    df["Swing_Low"] = False

    h = df["High"].values
    l = df["Low"].values
    n = len(df)

    if lookback == 2 and n > 5:
        is_sh = (h[2:-2] > h[:-4]) & (h[2:-2] > h[1:-3]) & (h[2:-2] > h[3:-1]) & (h[2:-2] > h[4:])
        is_sl = (l[2:-2] < l[:-4]) & (l[2:-2] < l[1:-3]) & (l[2:-2] < l[3:-1]) & (l[2:-2] < l[4:])
        df.iloc[2:-2, df.columns.get_loc("Swing_High")] = is_sh
        df.iloc[2:-2, df.columns.get_loc("Swing_Low")] = is_sl
        return df

    for i in range(lookback, n - lookback):
        hi = h[i]
        lo = l[i]
        if hi == np.max(h[i - lookback:i + lookback + 1]):
            df.iat[i, df.columns.get_loc("Swing_High")] = True
        if lo == np.min(l[i - lookback:i + lookback + 1]):
            df.iat[i, df.columns.get_loc("Swing_Low")] = True

    return df


def ensure_smc_columns(df: pd.DataFrame, copy: bool = True) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if copy:
        df = df.copy()
    if "Swing_High" not in df.columns or "Swing_Low" not in df.columns:
        df = detect_swings(df, lookback=2, copy=False)
    else:
        if df["Swing_High"].sum() == 0 and len(df) > 10:
            df = detect_swings(df, lookback=2, copy=False)
        if df["Swing_Low"].sum() == 0 and len(df) > 10:
            df = detect_swings(df, lookback=2, copy=False)
    return df


# ==============================================================================
# 1) MARKET STRUCTURE / OTE (simple)
# ==============================================================================

def compute_smc_levels(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {}
    df = ensure_smc_columns(df)

    last_sh_idx = df[df["Swing_High"]].last_valid_index()
    last_sl_idx = df[df["Swing_Low"]].last_valid_index()

    trend = "SIDEWAY"
    if last_sh_idx is not None and last_sl_idx is not None:
        trend = "UP" if last_sh_idx > last_sl_idx else "DOWN"

    ote_low, ote_high = 0.0, 0.0
    leg_type = "UNKNOWN"

    if trend == "UP" and last_sl_idx is not None:
        leg_low = float(df.loc[last_sl_idx]["Low"])
        leg_high = float(df.loc[last_sl_idx:]["High"].max())
        if leg_high > leg_low:
            diff = leg_high - leg_low
            ote_high = leg_low + diff * 0.79
            ote_low = leg_low + diff * 0.62
            leg_type = "BOS_UP"

    if trend == "DOWN" and last_sh_idx is not None:
        leg_high = float(df.loc[last_sh_idx]["High"])
        leg_low = float(df.loc[last_sh_idx:]["Low"].min())
        if leg_high > leg_low:
            diff = leg_high - leg_low
            ote_low = leg_high - diff * 0.79
            ote_high = leg_high - diff * 0.62
            leg_type = "BOS_DOWN"

    return {
        "trend": trend,
        "last_sh": last_sh_idx,
        "last_sl": last_sl_idx,
        "ote_low": float(ote_low),
        "ote_high": float(ote_high),
        "leg_type": leg_type,
        "bos_index": df.index[-1],
    }


# ==============================================================================
# 2) FVG & ORDER BLOCKS
# ==============================================================================

def detect_fvg_zones(df: pd.DataFrame, max_zones: int = 5, future_window: int = 60) -> list:
    """Detect Fair Value Gaps.

    Perf: limit the "future" invalidation scan to `future_window` bars to avoid O(n^2) behavior.
    """
    if df is None or df.empty or len(df) < 5:
        return []
    highs = df["High"].values
    lows = df["Low"].values
    dates = df.index
    avg_range = (df["High"] - df["Low"]).mean()
    min_gap = avg_range * 0.3
    zones = []
    
    for i in range(len(df) - 2, 2, -1):
        if len(zones) >= max_zones: break
        # Bullish FVG
        if highs[i - 2] < lows[i]:
            if (lows[i] - highs[i - 2]) > min_gap:
                future_lows = lows[i + 1:i + 1 + int(future_window)] if future_window else lows[i + 1:]
                mid = (lows[i] + highs[i-2]) / 2
                if not (len(future_lows) > 0 and np.min(future_lows) <= mid):
                    zones.append({"type": "FVG_BULL", "side": "bull", "y0": float(highs[i-2]), "y1": float(lows[i]), "bottom": float(highs[i-2]), "top": float(lows[i]), "start_idx": dates[i-2]})
        # Bearish FVG
        elif lows[i - 2] > highs[i]:
            if (lows[i - 2] - highs[i]) > min_gap:
                future_highs = highs[i + 1:i + 1 + int(future_window)] if future_window else highs[i + 1:]
                mid = (lows[i-2] + highs[i]) / 2
                if not (len(future_highs) > 0 and np.max(future_highs) >= mid):
                    zones.append({"type": "FVG_BEAR", "side": "bear", "y0": float(highs[i]), "y1": float(lows[i-2]), "bottom": float(highs[i]), "top": float(lows[i-2]), "start_idx": dates[i-2]})
    return zones


def detect_order_blocks(df: pd.DataFrame, lookback: int = 120, max_obs: int = 5) -> list:
    if df is None or df.empty or len(df) < 30:
        return []
    # Avoid extra copies in scan path (caller can copy once)
    df = ensure_smc_columns(df, copy=False)
    if lookback and len(df) > lookback:
        df = df.tail(lookback)
    
    closes = df["Close"].values; highs = df["High"].values; lows = df["Low"].values
    dates = df.index
    sw_highs = np.where(df["Swing_High"].values)[0]
    sw_lows = np.where(df["Swing_Low"].values)[0]
    obs = []

    # Bull OB
    for idx in sw_highs:
        price_peak = highs[idx]
        break_idx = np.where(closes[idx+1:] > price_peak)[0]
        if len(break_idx) > 0:
            actual = idx + 1 + break_idx[0]
            valid_lows = sw_lows[sw_lows < actual]
            if len(valid_lows) > 0:
                origin = valid_lows[-1]
                if closes[-1] > lows[origin]:
                    obs.append({"type": "OB_BULL", "side": "bull", "y0": float(lows[origin]), "y1": float(highs[origin]), "bottom": float(lows[origin]), "top": float(highs[origin]), "start_idx": dates[origin]})
    
    # Bear OB
    for idx in sw_lows:
        price_trough = lows[idx]
        break_idx = np.where(closes[idx+1:] < price_trough)[0]
        if len(break_idx) > 0:
            actual = idx + 1 + break_idx[0]
            valid_highs = sw_highs[sw_highs < actual]
            if len(valid_highs) > 0:
                origin = valid_highs[-1]
                if closes[-1] < highs[origin]:
                    obs.append({"type": "OB_BEAR", "side": "bear", "y0": float(lows[origin]), "y1": float(highs[origin]), "bottom": float(lows[origin]), "top": float(highs[origin]), "start_idx": dates[origin]})
                    
    obs = sorted(obs, key=lambda x: x["start_idx"])
    return obs[-max_obs:]


def detect_last_order_block(df: pd.DataFrame):
    obs = detect_order_blocks(df)
    if not obs: return None
    best_ob = obs[-1].copy()
    if best_ob.get("type") == "OB_BULL": best_ob["type"] = "demand"
    elif best_ob.get("type") == "OB_BEAR": best_ob["type"] = "supply"
    return best_ob


# ==============================================================================
# 3) TRENDLINES & CONFLUENCE
# ==============================================================================

def detect_trendlines(df: pd.DataFrame) -> list:
    if df is None or df.empty: return []
    df = ensure_smc_columns(df)
    lines = []
    # Simplified Trendlines logic
    highs = df[df["Swing_High"]].tail(2)
    if len(highs) == 2:
        if highs["High"].iloc[1] < highs["High"].iloc[0]:
            lines.append({"type": "line", "x0": highs.index[0], "y0": float(highs["High"].iloc[0]), "x1": highs.index[1], "y1": float(highs["High"].iloc[1]), "extend": True, "color": "red"})
    lows = df[df["Swing_Low"]].tail(2)
    if len(lows) == 2:
        if lows["Low"].iloc[1] > lows["Low"].iloc[0]:
            lines.append({"type": "line", "x0": lows.index[0], "y0": float(lows["Low"].iloc[0]), "x1": lows.index[1], "y1": float(lows["Low"].iloc[1]), "extend": True, "color": "green"})
    return lines


def annotate_zone_confluence(
    df: pd.DataFrame,
    zones,
    label_prefix: str,
    ma_keys=None,
    buffer_pct: float = 0.001,
):
    """Annotate zones with MA confluence in a **non-destructive** way.

    - Keeps input zone dict schema intact, only adds: confluence, strength, label.
    - Accepts a dict, list[dict], or None.
    - Does NOT mutate the original zones list/dicts.
    """
    if df is None or df.empty or zones is None:
        return [] if zones is None else (zones if isinstance(zones, list) else [zones])

    if ma_keys is None:
        # Cover both old app keys and core keys to keep consistent output.
        ma_keys = [
            "SMA_10",
            "SMA_20",
            "EMA_50",
            "SMA_50",
            "EMA_100",
            "SMA_100",
            "EMA_200",
            "SMA_200",
        ]

    # Build MA snapshot at the last candle
    current_mas = {}
    last = df.iloc[-1]
    for k in ma_keys:
        if k in df.columns:
            try:
                v = float(last[k])
                if v > 0:
                    current_mas[k] = v
            except Exception:
                continue

    def _in_zone(price: float, low: float, high: float) -> bool:
        lo = min(low, high) * (1 - buffer_pct)
        hi = max(low, high) * (1 + buffer_pct)
        return lo <= price <= hi

    zlist = zones if isinstance(zones, list) else [zones]
    out = []
    for z in zlist:
        if not isinstance(z, dict):
            continue
        low = z.get("y0", z.get("bottom"))
        high = z.get("y1", z.get("top"))
        nz = z.copy()
        if low is None or high is None or not current_mas:
            nz["confluence"] = []
            nz["strength"] = nz.get("strength", "NORMAL")
            nz["label"] = nz.get("label", label_prefix)
            out.append(nz)
            continue
        lowf, highf = float(min(low, high)), float(max(low, high))
        confluence = [ma for ma, p in current_mas.items() if _in_zone(float(p), lowf, highf)]
        nz["confluence"] = confluence
        if confluence:
            nz["strength"] = "STRONG"
            # Shorten MA names for compact labels
            short_mas = [m.replace("SMA_", "S").replace("EMA_", "E") for m in confluence]
            nz["label"] = f"{label_prefix} + {','.join(short_mas)}"
        else:
            nz["strength"] = "NORMAL"
            nz["label"] = label_prefix
        out.append(nz)
    return out


def detect_confluence_zones(df: pd.DataFrame, fvg_zones: list, ob_zone):
    """Backwards-compatible wrapper used by both Chart and Scanner.

    Notes:
    - Chart historically passes `ob_zone` as a **list** of OBs.
    - Scanner/VNINDEX report historically passes `ob_zone` as a **single dict**.
    This wrapper preserves the input type for OBs to avoid breaking downstream.
    """
    if df is None or df.empty:
        return [], [] if isinstance(ob_zone, list) else None

    strong_fvgs = annotate_zone_confluence(df, fvg_zones or [], label_prefix="FVG")

    if ob_zone is None:
        return strong_fvgs, [] if isinstance(ob_zone, list) else None

    # Preserve OB container type
    if isinstance(ob_zone, list):
        strong_obs = annotate_zone_confluence(df, ob_zone, label_prefix="OB")
        return strong_fvgs, strong_obs
    else:
        strong_ob_list = annotate_zone_confluence(df, ob_zone, label_prefix="OB")
        strong_ob = strong_ob_list[0] if strong_ob_list else None
        return strong_fvgs, strong_ob


# ==============================================================================
# 4) MSS / LIQUIDITY SWEEP / HELPERS
# ==============================================================================

def detect_mss(df: pd.DataFrame):
    if df is None or df.empty or len(df) < 20: return None
    df = ensure_smc_columns(df)
    last = df.iloc[-1]
    swings_high = df[df["Swing_High"]]
    swings_low = df[df["Swing_Low"]]
    
    if len(swings_high) >= 2 and float(last["Close"]) > float(swings_high.iloc[-2]["High"]): return "BULL"
    if len(swings_low) >= 2 and float(last["Close"]) < float(swings_low.iloc[-2]["Low"]): return "BEAR"
    return None

def detect_liquidity_sweep(df: pd.DataFrame, lookback: int = 20):
 
    if df is None or df.empty or len(df) < lookback + 2:
        return None

    recent = df.iloc[-lookback:-1]
    last = df.iloc[-1]

    # Quét đáy: Low thấp hơn đáy cũ và Close quay lại phía trên đáy cũ
    if float(last["Low"]) < float(recent["Low"].min()) and float(last["Close"]) > float(recent["Low"].min()):
        return "SELL_SIDE"

    # Quét đỉnh: High cao hơn đỉnh cũ và Close quay lại phía dưới đỉnh cũ
    if float(last["High"]) > float(recent["High"].max()) and float(last["Close"]) < float(recent["High"].max()):
        return "BUY_SIDE"

    return None

def detect_bpr(fvgs: list):
    bulls = [z for z in (fvgs or []) if z.get("side") == "bull"]
    bears = [z for z in (fvgs or []) if z.get("side") == "bear"]
    for b in bulls:
        for s in bears:
            low = max(float(b.get("bottom", b.get("y0"))), float(s.get("bottom", s.get("y0"))))
            high = min(float(b.get("top", b.get("y1"))), float(s.get("top", s.get("y1"))))
            if low < high: return {"low": low, "high": high}
    return None

def detect_smt(df_main: pd.DataFrame, df_pair: pd.DataFrame):
    if df_main is None or df_pair is None or len(df_main) < 3: return None
    # Simple Divergence
    if float(df_main.iloc[-1]["High"]) > float(df_main.iloc[-2]["High"]) and float(df_pair.iloc[-1]["High"]) <= float(df_pair.iloc[-2]["High"]): return "BEAR"
    if float(df_main.iloc[-1]["Low"]) < float(df_main.iloc[-2]["Low"]) and float(df_pair.iloc[-1]["Low"]) >= float(df_pair.iloc[-2]["Low"]): return "BULL"
    return None

# ==============================================================================
# 5) ENTRY MODELS (UPDATED & EXPANDED)
# ==============================================================================

# --- Model 1: Unicorn ---
def entry_ls_mss_bb_fvg(df: pd.DataFrame):
    sweep = detect_liquidity_sweep(df)
    mss = detect_mss(df)
    obs = detect_order_blocks(df)
    fvgs = detect_fvg_zones(df)
    if not sweep or not mss or not obs or not fvgs: return None

    ob = obs[-1]
    for fvg in fvgs:
        overlap_low = max(float(ob["y0"]), float(fvg.get("y0", fvg.get("bottom"))))
        overlap_high = min(float(ob["y1"]), float(fvg.get("y1", fvg.get("top"))))
        if overlap_low < overlap_high:
            side = "BUY" if mss == "BULL" else "SELL"
            if (side == "BUY" and sweep == "SELL_SIDE") or (side == "SELL" and sweep == "BUY_SIDE"):
                return {"entry": side, "zone": {"y0": overlap_low, "y1": overlap_high}, "model": "Unicorn (LS+BB+FVG)"}
    return None

# --- Model 2: ICT 2022 ---
def entry_ls_mss_fvg(df: pd.DataFrame):
    sweep = detect_liquidity_sweep(df)
    mss = detect_mss(df)
    fvgs = detect_fvg_zones(df)
    if sweep == "SELL_SIDE" and mss == "BULL":
        fvg = next((z for z in fvgs if z["side"] == "bull"), None)
        if fvg: return {"entry": "BUY", "zone": fvg, "model": "ICT 2022 (LS+MSS+FVG)"}
    if sweep == "BUY_SIDE" and mss == "BEAR":
        fvg = next((z for z in fvgs if z["side"] == "bear"), None)
        if fvg: return {"entry": "SELL", "zone": fvg, "model": "ICT 2022 (LS+MSS+FVG)"}
    return None

# --- [NEW] Model 3: OTE Pullback ---
def entry_ote_pullback(df: pd.DataFrame):
    smc = compute_smc_levels(df)
    if not smc:
        return None

    last_close = float(df.iloc[-1]["Close"])

    # Normalize vùng OTE để không bị đảo biên
    ote_low = float(smc["ote_low"])
    ote_high = float(smc["ote_high"])
    lo = min(ote_low, ote_high)
    hi = max(ote_low, ote_high)

    if smc.get("leg_type") == "BOS_UP":
        if lo <= last_close <= hi:
            return {"entry": "BUY", "zone": {"y0": lo, "y1": hi}, "model": "OTE Pullback (Fibo 0.7)"}

    if smc.get("leg_type") == "BOS_DOWN":
        if lo <= last_close <= hi:
            return {"entry": "SELL", "zone": {"y0": lo, "y1": hi}, "model": "OTE Pullback (Fibo 0.7)"}

    return None


# --- [NEW] Model 4: Breaker Block ---
def detect_breaker_blocks(df: pd.DataFrame):
    """
    Breaker block đơn giản:
    - Bull breaker: một nến giảm (prev bearish), sau đó nến sau đóng cửa phá lên trên đỉnh nến giảm đó.
    - Bear breaker: một nến tăng (prev bullish), sau đó nến sau đóng cửa phá xuống dưới đáy nến tăng đó.

    Return: list[dict] mỗi dict có keys: side, y0, y1, i
      y0 = đáy vùng breaker, y1 = đỉnh vùng breaker
    """
    breakers = []
    if df is None or len(df) < 3:
        return breakers

    # duyệt từ cuối về đầu để breaker mới nhất được append trước (giống logic FVG của bạn)
    for i in range(len(df) - 2, 1, -1):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]

        prev_open = float(prev["Open"])
        prev_close = float(prev["Close"])
        prev_high = float(prev["High"])
        prev_low = float(prev["Low"])

        cur_close = float(cur["Close"])

        # Bull breaker: prev bearish + cur close > prev high
        if prev_close < prev_open and cur_close > prev_high:
            breakers.append(
                {
                    "side": "bull",
                    "y0": prev_low,
                    "y1": prev_high,
                    "i": i,
                }
            )
            continue

        # Bear breaker: prev bullish + cur close < prev low
        if prev_close > prev_open and cur_close < prev_low:
            breakers.append(
                {
                    "side": "bear",
                    "y0": prev_low,
                    "y1": prev_high,
                    "i": i,
                }
            )
            continue

    return breakers
    
 
# --- [NEW] Model 5: Silver Bullet ---
def entry_silver_bullet(df: pd.DataFrame):
    fvgs = detect_fvg_zones(df)
    if not fvgs: 
        return None

    # detect_fvg_zones đang for i từ cuối về đầu và zones.append(...)
    # => phần tử ĐẦU (index 0) mới là zone "mới nhất"
    latest_fvg = fvgs[0]

    last_vol = float(df.iloc[-1]["Volume"])
    avg_vol = float(df["Volume"].tail(20).mean())
    if last_vol <= avg_vol * 1.5:
        return None

    last = df.iloc[-1]
    if latest_fvg["side"] == "bull":
        # retrace về FVG bull => BUY
        if float(last["Low"]) <= float(latest_fvg.get("y1", latest_fvg.get("top"))):
            return {"entry": "BUY", "zone": latest_fvg, "model": "Silver Bullet (Fresh FVG)"}

    if latest_fvg["side"] == "bear":
        # retrace về FVG bear => SELL
        if float(last["High"]) >= float(latest_fvg.get("y0", latest_fvg.get("bottom"))):
            return {"entry": "SELL", "zone": latest_fvg, "model": "Silver Bullet (Fresh FVG)"}

    return None



# --- [NEW] Model 6: AMD ---
def entry_amd_setup(df: pd.DataFrame):
    recent = df.iloc[-7:-2]
    if len(recent) < 5:
        return None

    range_pct = (recent["High"].max() - recent["Low"].min()) / recent["Low"].min()
    if range_pct >= 0.03:
        return None

    sweep = detect_liquidity_sweep(df, lookback=5)

    if sweep == "SELL_SIDE":
        # sweep low -> buy
        low = float(df.iloc[-1]["Low"])
        return {"entry": "BUY", "zone": {"y0": low, "y1": low * 1.01}, "model": "AMD (Power of 3)"}

    if sweep == "BUY_SIDE":
        # sweep high -> sell
        high = float(df.iloc[-1]["High"])
        return {"entry": "SELL", "zone": {"y0": high * 0.99, "y1": high}, "model": "AMD (Power of 3)"}

    return None


# --- Old Models (Kept for compatibility) ---
def entry_ls_bpr(df: pd.DataFrame):
    sweep = detect_liquidity_sweep(df)
    fvgs = detect_fvg_zones(df)
    bpr = detect_bpr(fvgs)
    if sweep == "SELL_SIDE" and bpr: return {"entry": "BUY", "zone": bpr, "model": "LS+BPR"}
    if sweep == "BUY_SIDE" and bpr: return {"entry": "SELL", "zone": bpr, "model": "LS+BPR"}
    return None

def entry_mss_fvg_simple(df: pd.DataFrame):
    mss = detect_mss(df)
    fvgs = detect_fvg_zones(df)
    if not fvgs:
        return None

    if mss == "BULL":
        fvg = next((z for z in fvgs if z["side"] == "bull"), None)
        if fvg:
            return {"entry": "BUY", "zone": fvg, "model": "MSS+FVG"}

    if mss == "BEAR":
        fvg = next((z for z in fvgs if z["side"] == "bear"), None)
        if fvg:
            return {"entry": "SELL", "zone": fvg, "model": "MSS+FVG"}

    return None


def entry_mss_ob_simple(df: pd.DataFrame):
    mss = detect_mss(df)
    obs = detect_order_blocks(df)
    if not obs:
        return None

    if mss == "BULL":
        ob = next((z for z in reversed(obs) if z["side"] == "bull"), None)
        if ob:
            return {"entry": "BUY", "zone": ob, "model": "MSS+OB"}

    if mss == "BEAR":
        ob = next((z for z in reversed(obs) if z["side"] == "bear"), None)
        if ob:
            return {"entry": "SELL", "zone": ob, "model": "MSS+OB"}

    return None

def entry_smt_mss_ifvg(df: pd.DataFrame, df_pair: pd.DataFrame):
    smt = detect_smt(df, df_pair)
    mss = detect_mss(df)
    fvgs = detect_fvg_zones(df)
    if not fvgs:
        return None

    if smt == "BULL" and mss == "BULL":
        fvg = next((z for z in fvgs if z["side"] == "bear"), None)
        if fvg:
            return {"entry": "BUY", "zone": fvg, "model": "SMT+MSS+IFVG"}

    if smt == "BEAR" and mss == "BEAR":
        fvg = next((z for z in fvgs if z["side"] == "bull"), None)
        if fvg:
            return {"entry": "SELL", "zone": fvg, "model": "SMT+MSS+IFVG"}

    return None


def entry_smt_mss_bb(df: pd.DataFrame, df_pair: pd.DataFrame):
    smt = detect_smt(df, df_pair)
    mss = detect_mss(df)
    obs = detect_order_blocks(df)
    if not obs:
        return None

    if smt == "BULL" and mss == "BULL":
        ob = obs[-1]
        if ob["side"] == "bull":
            return {"entry": "BUY", "zone": ob, "model": "SMT+MSS+BB"}

    if smt == "BEAR" and mss == "BEAR":
        ob = obs[-1]
        if ob["side"] == "bear":
            return {"entry": "SELL", "zone": ob, "model": "SMT+MSS+BB"}

    return None

def entry_bos_pullback(df_htf: pd.DataFrame, df_ltf: pd.DataFrame):
    if df_ltf is None or len(df_ltf) < 3: return None
    htf_mss = detect_mss(df_htf)
    ltf_mss = detect_mss(df_ltf)
    if htf_mss and ltf_mss == htf_mss:
        last = df_ltf.iloc[-1]; prev = df_ltf.iloc[-2]
        z_low = float(min(prev["Open"], prev["Close"], prev["Low"]))
        z_high = float(max(prev["Open"], prev["Close"], prev["High"]))
        # Bull: sweep below previous low (pullback) while structure aligns
        if htf_mss == "BULL" and float(last["Low"]) < float(prev["Low"]):
            return {"entry": "BUY", "zone": {"y0": z_low, "y1": z_high}, "model": "HTF_BOS_LTF_PULLBACK"}
        # Bear: sweep above previous high (pullback) while structure aligns
        if htf_mss == "BEAR" and float(last["High"]) > float(prev["High"]):
            return {"entry": "SELL", "zone": {"y0": z_low, "y1": z_high}, "model": "HTF_BOS_LTF_PULLBACK"}
    return None
    
# =========================
# ENTRY MODEL: Breaker Block Retest
# =========================
def entry_breaker_retest(df: pd.DataFrame):
    """
    Breaker Block Retest:
    - Bull breaker: phá lên -> retest -> BUY
    - Bear breaker: phá xuống -> retest -> SELL

    detect_breaker_blocks() PHẢI trả về list breaker:
    [{side, y0, y1, i}, ...]
    """
    breakers = detect_breaker_blocks(df)
    if not breakers:
        return None

    # breaker mới nhất
    brk = breakers[0]

    last = df.iloc[-1]
    last_low = float(last["Low"])
    last_high = float(last["High"])
    last_close = float(last["Close"])

    lo = min(float(brk["y0"]), float(brk["y1"]))
    hi = max(float(brk["y0"]), float(brk["y1"]))
    mid = (lo + hi) / 2.0

    if brk["side"] == "bull":
        if last_low <= hi and last_close >= lo:
            return {
                "entry": "BUY",
                "zone": {"y0": lo, "y1": hi},
                "model": "Breaker Block Retest"
            }

    if brk["side"] == "bear":
        if last_high >= lo and last_close <= hi:
            return {
                "entry": "SELL",
                "zone": {"y0": lo, "y1": hi},
                "model": "Breaker Block Retest"
            }


# ==============================================================================
# 6) AGGREGATOR & SCORING (UPDATED)
# ==============================================================================

def detect_entry_models(df_htf: pd.DataFrame, df_ltf=None, df_pair=None, return_artifacts: bool = False):
    """
    Tổng hợp tất cả các models theo thứ tự ưu tiên
    """
    # 1. Super Strong
    e = entry_ls_mss_bb_fvg(df_htf)
    if e:
        if return_artifacts:
            e = dict(e)
            # Attach HTF artifacts to avoid recomputation in scanner
            e["_ctx"] = {
                "fvgs": detect_fvg_zones(df_htf),
                "obs": detect_order_blocks(df_htf),
                "sweep": detect_liquidity_sweep(df_htf),
            }
        return e
    #e = entry_amd_setup(df_htf)
    #if e: return e

    # 2. Strong
    e = entry_silver_bullet(df_htf)
    if e:
        if return_artifacts:
            e = dict(e)
            e["_ctx"] = {
                "fvgs": detect_fvg_zones(df_htf),
                "obs": detect_order_blocks(df_htf),
                "sweep": detect_liquidity_sweep(df_htf),
            }
        return e
    e = entry_ls_mss_fvg(df_htf)
    if e:
        if return_artifacts:
            e = dict(e)
            e["_ctx"] = {
                "fvgs": detect_fvg_zones(df_htf),
                "obs": detect_order_blocks(df_htf),
                "sweep": detect_liquidity_sweep(df_htf),
            }
        return e
    e = entry_breaker_retest(df_htf)
    if e:
        if return_artifacts:
            e = dict(e)
            e["_ctx"] = {
                "fvgs": detect_fvg_zones(df_htf),
                "obs": detect_order_blocks(df_htf),
                "sweep": detect_liquidity_sweep(df_htf),
            }
        return e
    e = entry_ls_bpr(df_htf)
    if e:
        if return_artifacts:
            e = dict(e)
            e["_ctx"] = {
                "fvgs": detect_fvg_zones(df_htf),
                "obs": detect_order_blocks(df_htf),
                "sweep": detect_liquidity_sweep(df_htf),
            }
        return e

    # 3. Medium
    e = entry_ote_pullback(df_htf)
    if e:
        if return_artifacts:
            e = dict(e)
            e["_ctx"] = {
                "fvgs": detect_fvg_zones(df_htf),
                "obs": detect_order_blocks(df_htf),
                "sweep": detect_liquidity_sweep(df_htf),
            }
        return e
    
    # 4. SMT & Simple
    if df_pair is not None:
        e = entry_smt_mss_ifvg(df_htf, df_pair)
        if e:
            if return_artifacts:
                e = dict(e)
                e["_ctx"] = {
                    "fvgs": detect_fvg_zones(df_htf),
                    "obs": detect_order_blocks(df_htf),
                    "sweep": detect_liquidity_sweep(df_htf),
                }
            return e
        e = entry_smt_mss_bb(df_htf, df_pair)
        if e:
            if return_artifacts:
                e = dict(e)
                e["_ctx"] = {
                    "fvgs": detect_fvg_zones(df_htf),
                    "obs": detect_order_blocks(df_htf),
                    "sweep": detect_liquidity_sweep(df_htf),
                }
            return e
        
    e = entry_mss_fvg_simple(df_htf)
    if e:
        if return_artifacts:
            e = dict(e)
            e["_ctx"] = {
                "fvgs": detect_fvg_zones(df_htf),
                "obs": detect_order_blocks(df_htf),
                "sweep": detect_liquidity_sweep(df_htf),
            }
        return e
    e = entry_mss_ob_simple(df_htf)
    if e:
        if return_artifacts:
            e = dict(e)
            e["_ctx"] = {
                "fvgs": detect_fvg_zones(df_htf),
                "obs": detect_order_blocks(df_htf),
                "sweep": detect_liquidity_sweep(df_htf),
            }
        return e

    # 5. Trend Follow
    if df_ltf is not None:
        e = entry_bos_pullback(df_htf, df_ltf)
        if e:
            if return_artifacts:
                e = dict(e)
                e["_ctx"] = {
                    "fvgs": detect_fvg_zones(df_htf),
                    "obs": detect_order_blocks(df_htf),
                    "sweep": detect_liquidity_sweep(df_htf),
                }
            return e

    return None


ENTRY_SCORES = {
    "Unicorn (LS+BB+FVG)": 3,
    "AMD (Power of 3)": 3,
    "Silver Bullet (Fresh FVG)": 2.5,
    "ICT 2022 (LS+MSS+FVG)": 2,
    "Breaker Block Retest": 2,
    "LS+BPR": 2,
    "OTE Pullback (Fibo 0.7)": 2,
    "SMT+MSS+IFVG": 3,
    "SMT+MSS+BB": 3,
    "MSS+FVG": 1,
    "MSS+OB": 1,
    "HTF_BOS_LTF_PULLBACK": 1,
}

def calculate_advanced_score(row):
    """
    Hệ thống điểm Sniper Tối ưu: Thưởng mạnh cho sự cộng hưởng yếu tố.
    Thang điểm: 0.0 -> 5.0
    """
    score = 0.0
    notes = [] 

    # --- PHẦN 1: MÔ HÌNH SMC (Trọng số: 2.5 điểm) ---
    model = str(row.get('model_name', ''))
    
    # Thưởng cao cho các mô hình dòng tiền lớn
    if any(x in model for x in ["Unicorn", "AMD", "Silver Bullet", "SMT"]):
        score += 2.5
        notes.append("Mô hình S")
    elif any(x in model for x in ["ICT 2022", "Breaker", "OTE"]):
        score += 1.8
        notes.append("Mô hình A")
    elif any(x in model for x in ["MSS", "BOS", "LS"]):
        score += 1.0
        notes.append("Cấu trúc cơ bản")

    # --- PHẦN 2: PRICE ACTION (Trọng số: 1.5 điểm) ---
    pattern = str(row.get('candle_pattern', ''))
    
    if any(x in pattern for x in ['Engulfing', 'Fakey', 'OutsideBar']):
        score += 1.5
        notes.append("Lực nến mạnh")
    elif any(x in pattern for x in ['Pinbar', 'Morning_Star', 'Evening_Star']):
        score += 1.0
        notes.append("Nến xác nhận")
    elif any(x in pattern for x in ['RevBar', 'KeyRev']):
        score += 0.5
        notes.append("Đảo chiều nhẹ")

    # --- PHẦN 3: ĐIỂM CỘNG HƯỞNG & THÔNG MINH (Bonus: 1.0 điểm) ---
    # Nới lỏng điều kiện: Chỉ cần 1 trong 2 yếu tố cũng có điểm bonus
    has_vol_spike = row.get('vol_spike', False)
    has_sweep = row.get('liquidity_sweep', False)
    
    if has_vol_spike:
        score += 0.5
        notes.append("Dòng tiền vào")
    if has_sweep:
        score += 0.5
        notes.append("Quét thanh khoản")
        
    # Thưởng thêm cho độ dốc xu hướng (Nếu giá nằm trên/dưới MA tương ứng)
    # (Được truyền từ scanner vào row)
    if row.get('trend_confirm', False):
        score += 0.5
        notes.append("Đồng thuận xu hướng")

    final_score = float(np.clip(score, 0.0, 5.0))
    return pd.Series([final_score, " | ".join(notes)], index=['raw_score', 'auto_notes'])

# ==============================================================================
# 7) HELPERS (Compatibility)
# ==============================================================================
def get_discount_premium_zone(df, lookback=100):
    recent = df.tail(lookback)
    h = recent["High"].max(); l = recent["Low"].min(); eq = (h+l)/2
    return {"high": h, "low": l, "eq": eq}

def is_valid_discount_premium(df, side):
    z = get_discount_premium_zone(df)
    p = df.iloc[-1]["Close"]
    return p <= z["eq"] if side == "BUY" else p >= z["eq"]

def detect_high_prob_ob(df): return detect_order_blocks(df)
def detect_high_prob_fvg(df): return detect_fvg_zones(df)
def get_intraday_poe(df): return None