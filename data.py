# data.py
import os
import streamlit as st
import logging
import random
from datetime import datetime, timedelta, date
from config import now_vn
import time
import pandas as pd

# [SPONSOR] Import thư viện cao cấp
try:
    import importlib.util
    HAS_PREMIUM = importlib.util.find_spec("vnstock_data") is not None
except Exception:
    HAS_PREMIUM = False

# Cấu hình Log
logging.getLogger('urllib3').setLevel(logging.WARNING)

# --- SỬA LỖI TẠI ĐÂY: Đã xóa chữ 's' thừa ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "data_cache")
if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)

def _get_vnstock_key() -> str | None:
    k = os.getenv("VNSTOCK_API_KEY")
    if k: return k.strip()
    try:
        k2 = st.secrets.get("VNSTOCK_API_KEY", None)
        if k2: return str(k2).strip()
    except Exception: pass
    return None

def _lazy_vnstock_data():
    from vnstock_data import Quote, Trading
    return Quote, Trading
    
def calculate_full_indicators(df):
    if df is None or df.empty: return df
    try:
        if HAS_PREMIUM:
            import pandas_ta as ta
            df.ta.sma(length=10, append=True)
            df.ta.sma(length=20, append=True)
            df.ta.ema(length=50, append=True)
            df.ta.ema(length=200, append=True)
            df.ta.rsi(length=14, append=True)
            df['Vol_MA'] = df['Volume'].rolling(20).mean()
            # VSA Logic
            from indicators import apply_vsa
            df = apply_vsa(df)
    except Exception as e:
        print(f"Indicator Error: {e}")
    return df

def normalize_columns(df):
    if df is None or df.empty: return df
    col_map = {'time': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}
    df.rename(columns=col_map, inplace=True)
    return df

def _validate_ohlcv_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    required = ["Open", "High", "Low", "Close", "Volume"]
    for c in required:
        if c not in df.columns: return pd.DataFrame()
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
    except Exception: return pd.DataFrame()
    return df

def fetch_stock_data(symbol: str, start_str: str, end_str: str, interval: str, mode: str = "history"):
    import pandas as pd
    symbol = (symbol or "").strip().upper()
    if not symbol: return pd.DataFrame()

    key = _get_vnstock_key()
    if not key: return pd.DataFrame()
    os.environ["VNSTOCK_API_KEY"] = key

    try: Quote, Trading = _lazy_vnstock_data()
    except Exception: return pd.DataFrame()

    sources = ["tcbs", "vnd", "vci"] if mode == "history" else ["tcbs", "vci"]
    is_index = symbol in ["VNINDEX", "VN30", "HNX", "HNX30", "UPCOM", "VNXALL"]

    for src in sources:
        try:
            quote = Quote(source=src, symbol=symbol)
            temp = quote.history(start=start_str, end=end_str, interval=interval)
            if temp is None or temp.empty: continue

            df_res = normalize_columns(temp)
            if "Date" in df_res.columns:
                df_res["Date"] = pd.to_datetime(df_res["Date"])
                df_res.set_index("Date", inplace=True)
            
            if not isinstance(df_res.index, pd.DatetimeIndex):
                df_res.index = pd.to_datetime(df_res.index)

            if not is_index and "Close" in df_res.columns:
                try:
                    if df_res["Close"].mean() > 5000:
                        for c in ["Open", "High", "Low", "Close"]:
                            if c in df_res.columns: df_res[c] = df_res[c] / 1000.0
                except Exception: pass

            df_res = _validate_ohlcv_df(df_res)
            if df_res is None or df_res.empty: continue
            return df_res
        except Exception: continue
    return pd.DataFrame()

def load_data_with_cache(symbol, days_to_load=365, timeframe='1D', end_date=None):
    symbol = (symbol or "").strip().upper().replace(" ", "").replace(".VN", "").replace("^", "")
    symbol = "".join([ch for ch in symbol if ch.isalnum()])
    
    tf_map = {'1D': '1D', '1H': '1H', '15m': '15m'}
    interval = tf_map.get(timeframe, '1D')
    file_path = os.path.join(CACHE_DIR, f"{symbol}_{timeframe}.parquet")
    
    now = now_vn()    
    today_date = now.date()
    today_str = today_date.strftime("%Y-%m-%d")
    
    df_old = pd.DataFrame()
    last_cached_date = None
    
    if os.path.exists(file_path):
        try:
            df_old = pd.read_parquet(file_path)
            if not df_old.empty:
                if 'Date' in df_old.columns:
                    df_old['Date'] = pd.to_datetime(df_old['Date'])
                    df_old.set_index('Date', inplace=True)
                if not isinstance(df_old.index, pd.DatetimeIndex):
                     df_old.index = pd.to_datetime(df_old.index)
                last_cached_date = df_old.index.max()
        except: df_old = pd.DataFrame()

    if end_date is not None:
        if not df_old.empty:
            df_backtest = df_old[df_old.index <= end_date]
            return df_backtest.tail(days_to_load)
        else:
            today_str = end_date.strftime("%Y-%m-%d")

    need_update = True
    if last_cached_date and end_date is None:
        last_date_val = last_cached_date.date()
        if now.hour < 9:
            yesterday = today_date - timedelta(days=1)
            if last_date_val >= yesterday: need_update = False
        elif now.hour >= 15:
            if last_date_val == today_date: need_update = False
            
    if not need_update and not df_old.empty and end_date is None:
        return df_old.tail(days_to_load)

    start_date_str = None
    fetch_mode = 'history'
    
    if last_cached_date:
        days_gap = (today_date - last_cached_date.date()).days
        if days_gap == 0:
            start_date_str = today_str
            fetch_mode = 'realtime'
        else:
            start_date_str = (last_cached_date + timedelta(days=1)).strftime("%Y-%m-%d")
            fetch_mode = 'history' if days_gap > 3 else 'realtime'
    else:
        start_date_str = (now - timedelta(days=days_to_load + 10)).strftime("%Y-%m-%d")
        fetch_mode = 'history'

    if start_date_str > today_str: return df_old.tail(days_to_load)

    df_new = fetch_stock_data(symbol, start_date_str, today_str, interval, mode=fetch_mode)
    df_new = _validate_ohlcv_df(df_new)

    df_final = pd.DataFrame()
    if df_new is not None and not df_new.empty:
        if not df_old.empty:
            df_final = pd.concat([df_old, df_new])
            df_final = df_final[~df_final.index.duplicated(keep='last')]
        else:
            df_final = df_new
        
        df_final.sort_index(inplace=True)
        df_final = calculate_full_indicators(df_final)
        try: df_final.to_parquet(file_path)
        except: pass
    else:
        df_final = df_old

    if not df_final.empty:
        df_final = df_final[(df_final.index.dayofweek < 5) & (df_final['Volume'] > 0)]
        if end_date is not None:
            df_final = df_final[df_final.index <= end_date]
    
    return df_final.tail(days_to_load)

def load_smart_money_data(symbol):
    """
    Phiên bản Fail-safe: Nếu không có vnstock_data hoặc lỗi mạng, trả về DataFrame rỗng.
    Giúp App không bị sập khi deploy.
    """
    df_foreign = pd.DataFrame()
    df_prop = pd.DataFrame()
    df_depth = pd.DataFrame()
    
    if not HAS_PREMIUM:
        return df_foreign, df_prop, df_depth

    try:
        Quote, Trading = _lazy_vnstock_data()
        
        # 1. Trading (Foreign/Prop)
        try:
            trading_agent = Trading(source="vci", symbol=symbol)
            f_temp = trading_agent.foreign_trade()
            if f_temp is not None and not f_temp.empty:
                df_foreign = f_temp
            
            p_temp = trading_agent.prop_trade()
            if p_temp is not None and not p_temp.empty:
                df_prop = p_temp
        except: pass

        # 2. Quote (Depth)
        try:
            quote_vnd = Quote(source="vnd", symbol=symbol)
            d_temp = quote_vnd.price_depth()
            if d_temp is not None and not d_temp.empty:
                df_depth = d_temp
        except:
            try:
                quote_vci = Quote(source="vci", symbol=symbol)
                d_temp = quote_vci.price_depth()
                if d_temp is not None and not d_temp.empty:
                    df_depth = d_temp
            except: pass

    except Exception:
        pass

    return df_foreign, df_prop, df_depth