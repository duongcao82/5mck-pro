import os
import streamlit as st

def _get_vnstock_key() -> str | None:
    # Ưu tiên biến môi trường (Streamlit Cloud set rất tiện)
    k = os.getenv("VNSTOCK_API_KEY")
    if k:
        return k.strip()

    # Fallback: Streamlit secrets
    try:
        k2 = st.secrets.get("VNSTOCK_API_KEY", None)
        if k2:
            return str(k2).strip()
    except Exception:
        pass

    return None


import logging
import random
from datetime import datetime, timedelta, date
from config import now_vn
import time
import pandas as pd


# [SPONSOR] Import thư viện cao cấp
try:
    import importlib
    HAS_PREMIUM = importlib.util.find_spec("vnstock_data") is not None
except Exception:
    HAS_PREMIUM = False

    
# Cấu hình Log
logging.getLogger('urllib3').setLevel(logging.WARNING)
s
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "data_cache")
if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)

def _lazy_vnstock_data():
    # Import khi cần, tránh load nặng lúc startup
    from vnstock_data import Quote, Trading
    return Quote, Trading
    
def calculate_full_indicators(df):
    """
    Sử dụng vnstock_ta (Sponsor) để tính chỉ báo nhanh và chuẩn hơn.
    """
    if df is None or df.empty: return df
    
    # vnstock_ta yêu cầu index là datetime (tên cột 'time' hoặc index)
    # Đảm bảo df đang chuẩn
    
    try:
        # Nếu dùng vnstock_ta
        if HAS_PREMIUM:
            # Indicator class cần copy để không ảnh hưởng df gốc ngay lập tức
            # Lưu ý: vnstock_ta thường thao tác trực tiếp hoặc trả về series
            
            # Tính toán thủ công bằng pandas_ta như phương án fallback 
            # hoặc dùng Indicator wrapper nếu muốn đồng bộ
            # Ở đây tôi tích hợp pandas_ta (như code cũ) nhưng tối ưu hơn
            # Vì vnstock_ta chủ yếu wrap lại pandas_ta/ta-lib
            import pandas_ta as ta
            
            # MA
            df.ta.sma(length=10, append=True)
            df.ta.sma(length=20, append=True)
            df.ta.ema(length=50, append=True)
            df.ta.ema(length=100, append=True)
            df.ta.ema(length=200, append=True)
            # RSI
            df.ta.rsi(length=14, append=True)
            # Volume MA (cho VSA)
            df['Vol_MA'] = df['Volume'].rolling(20).mean()
            
            # VSA Logic (Giữ nguyên logic tùy chỉnh của bạn)
            from indicators import apply_vsa
            df = apply_vsa(df)
            
    except Exception as e:
        print(f"Indicator Error: {e}")
        
    return df

def normalize_columns(df):
    """Chuẩn hóa tên cột từ vnstock_data về chuẩn chung của App"""
    if df is None or df.empty: return df
    
    # vnstock_data trả về: time, open, high, low, close, volume
    # App cần: Date (index), Open, High, Low, Close, Volume (Viết hoa chữ cái đầu)
    
    col_map = {
        'time': 'Date', 
        'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'
    }
    df.rename(columns=col_map, inplace=True)
    return df


def _validate_ohlcv_df(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure OHLCV schema + DatetimeIndex (non-destructive).

    This project depends on keeping the original DataFrame structure stable.
    We only normalize/validate and **never** add/remove required OHLCV columns.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    required = ["Open", "High", "Low", "Close", "Volume"]
    for c in required:
        if c not in df.columns:
            return pd.DataFrame()
    # Ensure DateTimeIndex
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
    except Exception:
        return pd.DataFrame()
    return df

def fetch_stock_data(symbol: str, start_str: str, end_str: str, interval: str, mode: str = "history"):
    """
    Fetch OHLCV từ vnstock_data (Sponsor) theo chiến thuật fallback source.
    - Ưu tiên: lấy API key trước (ENV/Secrets), set env, rồi mới import vnstock_data (lazy import)
    - Fallback sources: tcbs -> vnd -> vci (history), tcbs -> vci -> vnd (realtime)
    - Chuẩn hoá cột + index DatetimeIndex + validate OHLCV
    - Trả về DataFrame rỗng nếu fail (không crash app)
    """
    import pandas as pd

    symbol = (symbol or "").strip().upper()
    if not symbol:
        return pd.DataFrame()

    # 1) Get key trước (tránh auth/treo khi chưa có key)
    key = _get_vnstock_key()
    if not key:
        return pd.DataFrame()

    # 2) Set env để thư viện đọc được (nếu nó dùng env)
    os.environ["VNSTOCK_API_KEY"] = key

    # 3) Lazy import vnstock_data
    try:
        Quote, Trading = _lazy_vnstock_data()
    except Exception:
        # Chưa cài vnstock_data trên Cloud hoặc import lỗi
        return pd.DataFrame()

    # 4) Strategy sources
    if mode == "history":
        sources = ["tcbs", "vnd", "vci"]
    else:
        sources = ["tcbs", "vci"]

    is_index = symbol in ["VNINDEX", "VN30", "HNX", "HNX30", "UPCOM", "VNXALL"]

    # 5) Try each source
    for src in sources:
        try:
            quote = Quote(source=src, symbol=symbol)

            temp = quote.history(start=start_str, end=end_str, interval=interval)
            if temp is None or temp.empty:
                continue

            df_res = normalize_columns(temp)

            # set index Date
            if "Date" in df_res.columns:
                df_res["Date"] = pd.to_datetime(df_res["Date"])
                df_res.set_index("Date", inplace=True)

            # Ensure DateTimeIndex
            if not isinstance(df_res.index, pd.DatetimeIndex):
                df_res.index = pd.to_datetime(df_res.index)

            # Fix đơn vị giá (một số nguồn trả đồng -> chia 1000)
            if not is_index and "Close" in df_res.columns:
                try:
                    if df_res["Close"].mean() > 5000:
                        cols = ["Open", "High", "Low", "Close"]
                        for c in cols:
                            if c in df_res.columns:
                                df_res[c] = df_res[c] / 1000.0
                except Exception:
                    pass

            # Validate schema OHLCV
            df_res = _validate_ohlcv_df(df_res)
            if df_res is None or df_res.empty:
                continue

            return df_res

        except Exception:
            # thử nguồn tiếp theo
            continue

    # Fail all
    return pd.DataFrame()


# ==============================================================================
# [CORE] LOAD DATA WITH SMART CACHE (INSIDER EDITION)
# ==============================================================================
def load_data_with_cache(symbol, days_to_load=365, timeframe='1D', end_date=None):
    """
    [TỐI ƯU CHO BACKTEST] Giữ nguyên logic Smart Cache, bổ sung tham số end_date
    để lọc dữ liệu khung nhỏ (H1, 15m) đúng thời điểm trong quá khứ.
    """
    symbol = (symbol or "").strip().upper()
    symbol = symbol.replace(" ", "").replace(".VN", "").replace("^", "")
    symbol = "".join([ch for ch in symbol if ch.isalnum()])
    
    tf_map = {'1D': '1D', '1H': '1H', '15m': '15m'}
    interval = tf_map.get(timeframe, '1D')
    
    file_path = os.path.join(CACHE_DIR, f"{symbol}_{timeframe}.parquet")
    
    now = now_vn()    
    today_date = now.date()
    today_str = today_date.strftime("%Y-%m-%d")
    
    df_old = pd.DataFrame()
    last_cached_date = None
    
    # 1. ĐỌC CACHE (GIỮ NGUYÊN)
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
        except: 
            df_old = pd.DataFrame()

    # --- LOGIC MỚI: NẾU ĐANG BACKTEST (CÓ END_DATE) ---
    # Nếu có end_date, chúng ta ưu tiên dùng dữ liệu đã có trong cache và CẮT nó
    if end_date is not None:
        if not df_old.empty:
            # Chỉ lấy dữ liệu TRƯỚC HOẶC BẰNG thời điểm end_date
            df_backtest = df_old[df_old.index <= end_date]
            # Trả về days_to_load nến cuối cùng tính từ end_date
            return df_backtest.tail(days_to_load)
        else:
            # Nếu cache trống, cho phép tải dữ liệu lịch sử đến end_date
            today_str = end_date.strftime("%Y-%m-%d")

    # 2. KIỂM TRA SKIP (GIỮ NGUYÊN)
    need_update = True
    if last_cached_date and end_date is None: # Chỉ skip update nếu không phải đang backtest
        last_date_val = last_cached_date.date()
        if now.hour < 9:
            yesterday = today_date - timedelta(days=1)
            if last_date_val >= yesterday: need_update = False
        elif now.hour >= 15:
            if last_date_val == today_date: need_update = False
            
    if not need_update and not df_old.empty and end_date is None:
        return df_old.tail(days_to_load)

    # 3. QUYẾT ĐỊNH TẢI (GIỮ NGUYÊN BIẾN NHƯNG CẬP NHẬT ĐIỀU KIỆN)
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
        # CHỖ CẦN SỬA: Tải đúng số ngày cần thiết + biên độ an toàn nhỏ (ví dụ 10 ngày)
        # thay vì + 200 ngày như cũ
        safety_margin = 10 
        start_date_str = (now - timedelta(days=days_to_load + safety_margin)).strftime("%Y-%m-%d")
        fetch_mode = 'history'

    # Tránh lỗi ngày bắt đầu lớn hơn ngày kết thúc
    if start_date_str > today_str: return df_old.tail(days_to_load)

    # 4. GỌI API SPONSOR (GIỮ NGUYÊN)
    df_new = fetch_stock_data(symbol, start_date_str, today_str, interval, mode=fetch_mode)
    df_new = _validate_ohlcv_df(df_new)

    # 5. MERGE & SAVE (GIỮ NGUYÊN)
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

    # 6. FILTER RÁC & TRẢ VỀ THEO END_DATE
    if not df_final.empty:
        df_final = df_final[(df_final.index.dayofweek < 5) & (df_final['Volume'] > 0)]
        if end_date is not None:
            df_final = df_final[df_final.index <= end_date]
    
    return df_final.tail(days_to_load)

# Thay thế TOÀN BỘ hàm load_smart_money_data cũ bằng đoạn code này:

# Thay thế hàm load_smart_money_data trong src/data.py bằng đoạn này:

# Thay thế hàm load_smart_money_data trong src/data.py

def load_smart_money_data(symbol):
    """
    Hàm lấy dữ liệu Dòng tiền (Phiên bản Fail-safe)
    Chấp nhận giới hạn của thư viện:
    - Trading: Chỉ dùng VCI (Nếu lỗi mạng -> Trả về rỗng, không crash)
    - Quote: Ưu tiên VND, dự phòng VCI.
    """
    # Khởi tạo mặc định là rỗng
    df_foreign = pd.DataFrame()
    df_prop = pd.DataFrame()
    df_depth = pd.DataFrame()
    '''
    # --- 1. LẤY DỮ LIỆU KHỐI NGOẠI & TỰ DOANH (Trading) ---
    # Bắt buộc dùng source='vci' do thư viện giới hạn.
    try:
        trading_agent = Trading(source="vci", symbol=symbol)
        
        # A. Khối ngoại (Foreign)
        try:
            df_foreign = trading_agent.foreign_trade()
            if df_foreign is not None and not df_foreign.empty:
                # Chuẩn hóa Index nếu có cột thời gian
                col_name = next((c for c in df_foreign.columns if c.lower() in ['date', 'time']), None)
                if col_name:
                    df_foreign[col_name] = pd.to_datetime(df_foreign[col_name])
                    df_foreign.set_index(col_name, inplace=True)
                    df_foreign.sort_index(inplace=True)
        except Exception:
            # Nếu VCI lỗi mạng/timeout -> Bỏ qua, giữ df_foreign rỗng
            pass 

        # B. Tự doanh (Proprietary)
        try:
            df_prop = trading_agent.prop_trade()
            if df_prop is not None and not df_prop.empty:
                col_name = next((c for c in df_prop.columns if c.lower() in ['date', 'time']), None)
                if col_name:
                    df_prop[col_name] = pd.to_datetime(df_prop[col_name])
                    df_prop.set_index(col_name, inplace=True)
        except Exception:
            pass

    except Exception:
        # Lỗi khởi tạo Trading (hiếm gặp)
        pass

    # --- 2. LẤY BƯỚC GIÁ (Quote Depth) ---
    # Thử VND trước (thường ổn định hơn VCI), nếu không được thì thử VCI
    try:
        # Ưu tiên 1: VND
        quote_vnd = Quote(source="vnd", symbol=symbol)
        df_depth = quote_vnd.price_depth()
    except Exception:
        # Ưu tiên 2: VCI (Fallback)
        try:
            quote_vci = Quote(source="vci", symbol=symbol)
            df_depth = quote_vci.price_depth()
        except Exception:
            pass # Cả 2 đều lỗi -> Trả về rỗng
'''
    return df_foreign, df_prop, df_depth