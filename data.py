# data.py - PHIÊN BẢN SẠCH & TỐI ƯU CACHE
import os
import streamlit as st
import logging
import pandas as pd
from datetime import datetime, timedelta
from config import now_vn

# [SPONSOR] Import thư viện cao cấp (nếu có)
try:
    import importlib.util
    HAS_PREMIUM = importlib.util.find_spec("vnstock_data") is not None
except Exception:
    HAS_PREMIUM = False

# Cấu hình Log để đỡ rác
logging.getLogger('urllib3').setLevel(logging.WARNING)

# --- ĐƯỜNG DẪN DỮ LIỆU ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "data_cache")

# Tự động tạo folder nếu chưa có (để tránh lỗi path)
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def _get_vnstock_key() -> str | None:
    # Ưu tiên lấy từ biến môi trường (Environment Variable)
    k = os.getenv("VNSTOCK_API_KEY")
    if k: return k.strip()
    # Sau đó lấy từ Streamlit secrets
    try:
        k2 = st.secrets.get("VNSTOCK_API_KEY", None)
        if k2: return str(k2).strip()
    except Exception: pass
    return None

def _lazy_vnstock_data():
    from vnstock_data import Quote, Trading
    return Quote, Trading

def calculate_full_indicators(df):
    """Tính toán chỉ báo kỹ thuật cơ bản"""
    if df is None or df.empty: return df
    try:
        # Nếu dùng pandas_ta
        import pandas_ta as ta
        df.ta.sma(length=20, append=True)
        df.ta.ema(length=50, append=True)
        df.ta.rsi(length=14, append=True)
        # Tính MA Vol
        df['Vol_MA'] = df['Volume'].rolling(20).mean()
    except Exception:
        pass # Bỏ qua lỗi chỉ báo để không sập app
    return df


# =========================
# FAST CACHE READER (SCAN)
# =========================
@st.cache_data(show_spinner=False)
def _read_parquet_tail_cached(path: str, mtime: float, tail_n: int) -> pd.DataFrame:
    """Read parquet quickly and return only the last N rows.

    - `mtime` is included to invalidate cache when the file changes.
    - This function is *read-only* and never triggers any API calls.
    """
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
        if df is None or df.empty:
            return pd.DataFrame()

        # Normalize index
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df.set_index("Date", inplace=True)
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors="coerce")

        df = df.sort_index()
        if tail_n and tail_n > 0:
            df = df.tail(int(tail_n))
        return df
    except Exception:
        return pd.DataFrame()


def read_cache_fast(symbol: str, timeframe: str = "1D", tail_n: int = 300, end_date=None) -> pd.DataFrame:
    """Fast path for scanner: only read local parquet cache.

    - Never calls API.
    - Never computes indicators.
    - Returns only tail_n rows (after optional end_date cut).
    """
    symbol = (symbol or "").strip().upper()
    symbol_clean = "".join([ch for ch in symbol if ch.isalnum()])
    tf = (timeframe or "1D").strip()
    file_path = os.path.join(CACHE_DIR, f"{symbol_clean}_{tf}.parquet")

    if not os.path.exists(file_path):
        return pd.DataFrame()

    try:
        mtime = os.path.getmtime(file_path)
    except Exception:
        mtime = 0.0

    df = _read_parquet_tail_cached(file_path, mtime, int(tail_n) if tail_n else 0)
    df = _validate_ohlcv_df(df)
    if df is None or df.empty:
        return pd.DataFrame()

    if end_date is not None:
        try:
            df = df[df.index <= end_date]
        except Exception:
            pass
    return df

def normalize_columns(df):
    """Chuẩn hóa tên cột về chuẩn chung"""
    if df is None or df.empty: return df
    # Map tên cột thường gặp về chuẩn
    col_map = {
        'time': 'Date', 'open': 'Open', 'high': 'High', 
        'low': 'Low', 'close': 'Close', 'volume': 'Volume'
    }
    df.rename(columns=col_map, inplace=True)
    return df

def _validate_ohlcv_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    required = ["Open", "High", "Low", "Close", "Volume"]
    # Kiểm tra thiếu cột
    for c in required:
        if c not in df.columns: return pd.DataFrame()
    # Kiểm tra Index
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
    except Exception: return pd.DataFrame()
    return df

def fetch_stock_data(symbol: str, start_str: str, end_str: str, interval: str, mode: str = "history"):
    """Hàm gọi API thực tế (fallback khi không có cache)"""
    symbol = (symbol or "").strip().upper()
    if not symbol: return pd.DataFrame()

    # Setup API Key
    key = _get_vnstock_key()
    if key: os.environ["VNSTOCK_API_KEY"] = key

    try: Quote, Trading = _lazy_vnstock_data()
    except Exception: return pd.DataFrame()

    # Ưu tiên nguồn: TCBS (nhanh) -> VCI (đủ) -> VND (backtest)
    sources = ["tcbs", "vci"] 
    if mode == "history": sources = ["vnd", "tcbs", "vci"]

    is_index = symbol in ["VNINDEX", "VN30", "HNX", "HNX30", "UPCOM"]

    for src in sources:
        try:
            quote = Quote(source=src, symbol=symbol)
            temp = quote.history(start=start_str, end=end_str, interval=interval)
            
            if temp is None or temp.empty: continue

            df_res = normalize_columns(temp)
            
            # Xử lý Index Date
            if "Date" in df_res.columns:
                df_res["Date"] = pd.to_datetime(df_res["Date"])
                df_res.set_index("Date", inplace=True)
            
            if not isinstance(df_res.index, pd.DatetimeIndex):
                df_res.index = pd.to_datetime(df_res.index)

            # Xử lý lỗi đơn vị giá (VND thường trả về giá * 1000)
            if not is_index and "Close" in df_res.columns:
                try:
                    if df_res["Close"].mean() > 5000: # Giá > 5000 thường là chưa chia 1000
                        for c in ["Open", "High", "Low", "Close"]:
                            if c in df_res.columns: df_res[c] = df_res[c] / 1000.0
                except Exception: pass

            df_res = _validate_ohlcv_df(df_res)
            if df_res is None or df_res.empty: continue
            
            return df_res # Trả về ngay khi lấy được
        except Exception: 
            continue
            
    return pd.DataFrame()

def load_data_with_cache(
    symbol,
    days_to_load: int = 365,
    timeframe: str = '1D',
    end_date=None,
    compute_indicators: bool = False,
    allow_fetch: bool = True,
):
    """Smart loader (cache-first, fill-gap).

    - Reads Parquet cache first.
    - Only fetches missing gap when `allow_fetch=True`.
    - Indicators are optional (scanner should keep this False).
    """
    symbol = (symbol or "").strip().upper()
    symbol_clean = "".join([ch for ch in symbol if ch.isalnum()])

    # Normalize timeframe -> interval for vnstock_data
    tf = (timeframe or "1D").strip()
    tf_map = {
        "1D": "1D", "D": "1D", "DAY": "1D",
        "1H": "1H", "H": "1H", "60": "1H",
        "15M": "15m", "15m": "15m", "15": "15m",
    }
    interval = tf_map.get(tf, tf)
    tf_norm = tf  # keep file naming stable with caller

    file_path = os.path.join(CACHE_DIR, f"{symbol_clean}_{tf_norm}.parquet")

    now = now_vn()
    today_date = now.date()
    today_str = today_date.strftime("%Y-%m-%d")

    df_old = pd.DataFrame()
    last_cached_date = None

    # --- STEP 1: read cache ---
    if os.path.exists(file_path):
        try:
            df_old = pd.read_parquet(file_path)
            if df_old is not None and not df_old.empty:
                if "Date" in df_old.columns:
                    df_old["Date"] = pd.to_datetime(df_old["Date"], errors="coerce")
                    df_old.set_index("Date", inplace=True)
                if not isinstance(df_old.index, pd.DatetimeIndex):
                    df_old.index = pd.to_datetime(df_old.index, errors="coerce")
                df_old = df_old.sort_index()
                last_cached_date = df_old.index.max()
        except Exception:
            df_old = pd.DataFrame()

    # Backtest cut (never fetch beyond end_date)
    if end_date is not None:
        if df_old is not None and not df_old.empty:
            try:
                df_bt = df_old[df_old.index <= end_date]
                return df_bt.tail(days_to_load)
            except Exception:
                return df_old.tail(days_to_load)
        today_str = end_date.strftime("%Y-%m-%d")

    # If scan-mode disallows fetch, return cache tail only
    if not allow_fetch:
        return df_old.tail(days_to_load) if df_old is not None else pd.DataFrame()

    # --- STEP 2: decide update ---
    need_update = True
    if last_cached_date is not None and end_date is None:
        last_date_val = last_cached_date.date()
        # Before market opens: don't refetch if we already have yesterday
        if now.hour < 9:
            yesterday = today_date - timedelta(days=1)
            if last_date_val >= yesterday:
                need_update = False
        # After market opens: if cache already includes today, we can skip (you can add intraday TTL here)
        elif last_date_val == today_date:
            need_update = False

    if not need_update and df_old is not None and not df_old.empty:
        return df_old.tail(days_to_load)

    # --- STEP 3: fetch missing gap ---
    if last_cached_date is not None:
        days_gap = (today_date - last_cached_date.date()).days
        if days_gap <= 0:
            start_date_str = today_str
            fetch_mode = "history"
        else:
            start_date_str = (last_cached_date + timedelta(days=1)).strftime("%Y-%m-%d")
            fetch_mode = "history"
    else:
        start_date_str = (now - timedelta(days=days_to_load + 20)).strftime("%Y-%m-%d")
        fetch_mode = "history"

    if start_date_str > today_str:
        return df_old.tail(days_to_load)

    df_new = fetch_stock_data(symbol, start_date_str, today_str, interval, mode=fetch_mode)
    df_new = _validate_ohlcv_df(df_new)

    # --- STEP 4: merge + persist ---
    if df_new is not None and not df_new.empty:
        if df_old is not None and not df_old.empty:
            df_final = pd.concat([df_old, df_new])
            df_final = df_final[~df_final.index.duplicated(keep="last")]
        else:
            df_final = df_new
        df_final = df_final.sort_index()

        if compute_indicators:
            df_final = calculate_full_indicators(df_final)

        try:
            df_final.to_parquet(file_path)
        except Exception:
            pass
    else:
        df_final = df_old

    # Final clean
    if df_final is not None and not df_final.empty:
        try:
            df_final = df_final[(df_final.index.dayofweek < 5) & (df_final["Volume"] > 0)]
        except Exception:
            pass

    return (df_final.tail(days_to_load) if df_final is not None else pd.DataFrame())

def load_smart_money_data(symbol):
    """Placeholder an toàn cho Smart Money"""
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()