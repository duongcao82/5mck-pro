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

def load_data_with_cache(symbol, days_to_load=365, timeframe='1D', end_date=None):
    """
    Hàm load dữ liệu thông minh:
    1. Đọc từ file Parquet (Cache Git) TRƯỚC.
    2. Chỉ gọi API để lấy phần dữ liệu CÒN THIẾU (Gap).
    """
    symbol = (symbol or "").strip().upper()
    # Làm sạch tên file (bỏ ký tự lạ)
    symbol_clean = "".join([ch for ch in symbol if ch.isalnum()])
    
    file_path = os.path.join(CACHE_DIR, f"{symbol_clean}_{timeframe}.parquet")
    
    now = now_vn()    
    today_date = now.date()
    today_str = today_date.strftime("%Y-%m-%d")
    
    df_old = pd.DataFrame()
    last_cached_date = None
    
    # --- BƯỚC 1: ĐỌC CACHE TỪ DISK (CỰC NHANH) ---
    if os.path.exists(file_path):
        try:
            df_old = pd.read_parquet(file_path)
            if not df_old.empty:
                # Đảm bảo index là Datetime
                if 'Date' in df_old.columns:
                    df_old['Date'] = pd.to_datetime(df_old['Date'])
                    df_old.set_index('Date', inplace=True)
                if not isinstance(df_old.index, pd.DatetimeIndex):
                     df_old.index = pd.to_datetime(df_old.index)
                
                df_old.sort_index(inplace=True)
                last_cached_date = df_old.index.max()
        except: 
            df_old = pd.DataFrame() # File lỗi thì coi như không có

    # Nếu chỉ cần backtest đến quá khứ và cache đã đủ -> Trả về luôn
    if end_date is not None:
        if not df_old.empty:
            df_backtest = df_old[df_old.index <= end_date]
            return df_backtest.tail(days_to_load)
        else:
            today_str = end_date.strftime("%Y-%m-%d")

    # --- BƯỚC 2: KIỂM TRA CÓ CẦN GỌI API KHÔNG? ---
    need_update = True
    
    if last_cached_date and end_date is None:
        last_date_val = last_cached_date.date()
        # Nếu cache là hôm qua, mà giờ mới 8h sáng -> Chưa có dữ liệu mới -> Không cần update
        if now.hour < 9:
            yesterday = today_date - timedelta(days=1)
            if last_date_val >= yesterday: need_update = False
        # Nếu cache đã là hôm nay -> Không cần update
        elif last_date_val == today_date and now.hour > 9: # Sau 9h mới cần check real-time
             # Ở đây có thể tùy chỉnh: ví dụ cache 15 phút
             pass 
            
    if not need_update and not df_old.empty:
        return df_old.tail(days_to_load)

    # --- BƯỚC 3: GỌI API LẤY PHẦN CÒN THIẾU ---
    start_date_str = None
    fetch_mode = 'history'
    
    if last_cached_date:
        # Chỉ tải từ ngày tiếp theo của cache
        days_gap = (today_date - last_cached_date.date()).days
        if days_gap <= 0:
            start_date_str = today_str # Load lại hôm nay (realtime)
            fetch_mode = 'history' # Dùng history cho ổn định, hoặc realtime nếu cần
        else:
            start_date_str = (last_cached_date + timedelta(days=1)).strftime("%Y-%m-%d")
            fetch_mode = 'history'
    else:
        # Nếu chưa có cache, tải full
        start_date_str = (now - timedelta(days=days_to_load + 20)).strftime("%Y-%m-%d")
        fetch_mode = 'history'

    # Nếu ngày bắt đầu lớn hơn hôm nay -> Không có gì để tải
    if start_date_str > today_str: 
        return df_old.tail(days_to_load)

    # Gọi hàm fetch (đã định nghĩa ở trên)
    df_new = fetch_stock_data(symbol, start_date_str, today_str, interval, mode=fetch_mode)
    df_new = _validate_ohlcv_df(df_new)

    # --- BƯỚC 4: GHÉP (MERGE) VÀ LƯU LẠI ---
    df_final = pd.DataFrame()
    if df_new is not None and not df_new.empty:
        if not df_old.empty:
            # Ghép mới vào cũ
            df_final = pd.concat([df_old, df_new])
            # Xóa trùng lặp (giữ cái mới nhất)
            df_final = df_final[~df_final.index.duplicated(keep='last')]
        else:
            df_final = df_new
        
        df_final.sort_index(inplace=True)
        # Tính lại chỉ báo cho chắc
        df_final = calculate_full_indicators(df_final)
        
        # Lưu ngược vào Cache (để lần sau nhanh hơn)
        # Lưu ý: Trên Streamlit Cloud, file này sẽ mất khi reboot, nhưng sẽ giúp nhanh trong phiên làm việc
        try: df_final.to_parquet(file_path)
        except: pass
    else:
        df_final = df_old

    # Lọc cuối cùng
    if not df_final.empty:
        # Bỏ ngày nghỉ (T7, CN) và Vol=0
        df_final = df_final[(df_final.index.dayofweek < 5) & (df_final['Volume'] > 0)]
        if end_date is not None:
            df_final = df_final[df_final.index <= end_date]
    
    return df_final.tail(days_to_load)

def load_smart_money_data(symbol):
    """Placeholder an toàn cho Smart Money"""
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()