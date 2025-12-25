# src/indicators.py
import pandas as pd
import pandas_ta as ta
import numpy as np

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df

    # --- 1. Moving Averages ---
    df.ta.sma(length=10, append=True)
    df.ta.sma(length=20, append=True)
    df.ta.ema(length=50, append=True)
    df.ta.sma(length=200, append=True)

    # --- 2. RSI ---
    df.ta.rsi(length=14, append=True)

    # --- 3. Bollinger Bands (SỬA LẠI ĐỂ ĐẢM BẢO TÊN CỘT) ---
    # Tính toán BB nhưng không append tự động, mà lấy kết quả để gán tên thủ công
    bb = df.ta.bbands(length=20, std=2)
    
    if bb is not None and not bb.empty:
        # pandas_ta trả về DF gồm: BBL (Lower), BBM (Mid), BBU (Upper), ...
        # Chúng ta tìm cột bắt đầu bằng BBL và BBU để gán vào df chính
        col_lower = [c for c in bb.columns if c.startswith("BBL")][0]
        col_upper = [c for c in bb.columns if c.startswith("BBU")][0]
        
        df["BB_LOW"] = bb[col_lower]
        df["BB_UP"]  = bb[col_upper]

    # --- 4. MACD ---
    df.ta.macd(fast=12, slow=26, signal=9, append=True)

    # --- 5. Volume SMA ---
    if "Volume" in df.columns:
        df["VOL_SMA_10"] = df["Volume"].rolling(10).mean()

    return df

def apply_vsa(df: pd.DataFrame, vol_len: int = 20) -> pd.DataFrame:
    # (Giữ nguyên code cũ)
    df = df.copy()
    if "Volume" not in df.columns:
        df["VSA_Signal"] = "Normal"
        return df
    df["Spread"] = df["High"] - df["Low"]
    df["Vol_MA"] = df["Volume"].rolling(vol_len).mean()
    spread_mean = df["Spread"].mean()
    spread_std = df["Spread"].std()
    wide_spread = spread_mean + spread_std
    narrow_spread = spread_mean - 0.5 * spread_std
    up_bar = df["Close"] > df["Open"]
    down_bar = df["Close"] < df["Open"]
    wide = df["Spread"] >= wide_spread
    narrow = df["Spread"] <= narrow_spread
    high_vol = df["Volume"] >= df["Vol_MA"] * 1.5
    low_vol = df["Volume"] <= df["Vol_MA"] * 0.7
    df["VSA_Signal"] = "Normal"
    df.loc[wide & up_bar & high_vol, "VSA_Signal"] = "Buying Climax"
    df.loc[wide & down_bar & high_vol, "VSA_Signal"] = "Selling Climax"
    df.loc[narrow & up_bar & low_vol, "VSA_Signal"] = "No Demand"
    df.loc[narrow & down_bar & low_vol, "VSA_Signal"] = "No Supply"
    return df

def detect_rsi_divergence(df: pd.DataFrame, lookback: int = 30):
    # (Giữ nguyên code cũ)
    if "RSI_14" not in df.columns or len(df) < 10: return []
    rsi = df["RSI_14"].values
    close = df["Close"].values
    n = len(df)
    start = max(1, n - lookback)
    price_high_idx, price_low_idx = [], []
    for i in range(start, n - 1):
        if close[i] > close[i - 1] and close[i] > close[i + 1]: price_high_idx.append(i)
        if close[i] < close[i - 1] and close[i] < close[i + 1]: price_low_idx.append(i)
    divergences = []
    for i in range(len(price_low_idx) - 1):
        idx1 = price_low_idx[i]; idx2 = price_low_idx[i+1]
        if close[idx2] < close[idx1] and rsi[idx2] > rsi[idx1]: divergences.append({"type": "bull", "i1": idx1, "i2": idx2})
    for i in range(len(price_high_idx) - 1):
        idx1 = price_high_idx[i]; idx2 = price_high_idx[i+1]
        if close[idx2] > close[idx1] and rsi[idx2] < rsi[idx1]: divergences.append({"type": "bear", "i1": idx1, "i2": idx2})
    return divergences

# ==============================================================================
# 5. PRICE ACTION (NẾN ĐẢO CHIỀU) - LOGIC TỪ AMIBROKER
# ==============================================================================
import numpy as np

def detect_price_action(df: pd.DataFrame):
    """
    Phát hiện các mẫu nến Price Action (PA) đảo chiều và tiếp diễn.
    """
    if df is None or df.empty or len(df) < 15:
        return df

    df = df.copy()
    
    # 1. Chuẩn bị dữ liệu cơ bản
    high = df['High']
    low = df['Low']
    close = df['Close']
    open_ = df['Open']
    
    # Tính ATR nếu chưa có (dùng cho Pinbar)
    if 'ATRr_14' not in df.columns:
        df.ta.atr(length=14, append=True)
    atr = df.get('ATRr_14', df['High'] - df['Low']) # Fallback nếu lỗi

    # Các thành phần nến
    bar_range = high - low
    upper_wick = high - np.maximum(open_, close)
    lower_wick = np.minimum(open_, close) - low
    
    # --- LOGIC PATTERNS ---

    # 1. Reversal Bar
    df['PA_RevBar_Bull'] = (low < low.shift(1)) & (close > open_)
    df['PA_RevBar_Bear'] = (high > high.shift(1)) & (close < open_)

    # 2. Key Reversal Bar (Mạnh hơn loại 1)
    df['PA_KeyRev_Bull'] = (open_ < open_.shift(1)) & (close > close.shift(1))
    df['PA_KeyRev_Bear'] = (open_ > open_.shift(1)) & (close < close.shift(1))

    # 3. Pin Bar (Đuôi dài)
    # Bull: Đuôi dưới dài > 60% thân, đuôi trên ngắn
    df['PA_PinBar_Bull'] = (bar_range > atr * 0.5) & \
                           (lower_wick >= 0.6 * bar_range) & \
                           (upper_wick <= 0.3 * bar_range)
    
    # Bear: Đuôi trên dài > 60% thân
    df['PA_PinBar_Bear'] = (bar_range > atr * 0.5) & \
                           (upper_wick >= 0.6 * bar_range) & \
                           (lower_wick <= 0.3 * bar_range)

    # 4. Inside Bar
    df['PA_InsideBar'] = (high < high.shift(1)) & (low > low.shift(1))

    # 5. Outside Bar (Engulfing)
    df['PA_OutsideBar'] = (high > high.shift(1)) & (low < low.shift(1))
    # Engulfing Bullish chuẩn
    df['PA_Engulf_Bull'] = (open_ < close.shift(1)) & (close > open_.shift(1)) & (close > open_)
    # Engulfing Bearish chuẩn
    df['PA_Engulf_Bear'] = (open_ > close.shift(1)) & (close < open_.shift(1)) & (close < open_)

    # 6. NR7 / NR4 (Nến biên độ hẹp - Nén giá)
    range_val = high - low
    df['PA_NR7'] = range_val == range_val.rolling(7).min()
    df['PA_NR4'] = range_val == range_val.rolling(4).min()

    # 7. Fakey (Inside Bar bị phá vỡ giả)
    prev_inside = df['PA_InsideBar'].shift(1).fillna(False)
    prev_high = high.shift(1)
    prev_low = low.shift(1)
    
    # Bull Fakey: Quét Low nến Inside trước đó rồi đóng cửa cao hơn
    df['PA_Fakey_Bull'] = prev_inside & (low < prev_low) & (close > prev_high)
    # Bear Fakey: Quét High nến Inside trước đó rồi đóng cửa thấp hơn
    df['PA_Fakey_Bear'] = prev_inside & (high > prev_high) & (close < prev_low)

    return df