# src/ict_core.py
import pandas as pd
import numpy as np
from datetime import time

# ==============================================================================
# 1. INDUCEMENT (IDM) - VÙNG DỤ THANH KHOẢN
# ==============================================================================
def detect_inducement(df: pd.DataFrame, trend_side: str):
    """
    Xác định điểm Inducement (IDM) gần nhất.
    - Setup BUY (Trend UP): IDM là Swing Low gần nhất nằm dưới giá hiện tại (nhưng trên OB).
    - Setup SELL (Trend DOWN): IDM là Swing High gần nhất nằm trên giá hiện tại (nhưng dưới OB).
    """
    if df.empty or "Swing_High" not in df.columns:
        return None

    # Lấy danh sách các điểm Swing
    swing_highs = df[df["Swing_High"]]
    swing_lows = df[df["Swing_Low"]]
    
    current_price = df.iloc[-1]["Close"]
    idm_point = None

    if trend_side == "BUY":
        # Tìm Swing Low gần nhất phía trên vùng Demand tiềm năng
        # Logic đơn giản: Lấy Swing Low gần nhất so với hiện tại
        if not swing_lows.empty:
            # Lấy Swing Low gần nhất về mặt thời gian
            last_sl = swing_lows.iloc[-1]
            # IDM hợp lệ thường phải nằm gần giá hiện tại
            idm_point = {
                "type": "IDM_LOW",
                "price": last_sl["Low"],
                "index": last_sl.name, # Datetime index
                "is_swept": current_price < last_sl["Low"] # True nếu giá đã quét qua
            }

    elif trend_side == "SELL":
        # Tìm Swing High gần nhất
        if not swing_highs.empty:
            last_sh = swing_highs.iloc[-1]
            idm_point = {
                "type": "IDM_HIGH",
                "price": last_sh["High"],
                "index": last_sh.name,
                "is_swept": current_price > last_sh["High"]
            }
            
    return idm_point

# ==============================================================================
# 2. KILLZONES - KHUNG GIỜ VÀNG (VNINDEX VERSION)
# ==============================================================================
def check_vnindex_killzone(current_time: pd.Timestamp):
    """
    Xác định xem nến hiện tại có nằm trong Killzone của VNI không.
    """
    t = current_time.time()
    
    # 1. ATO Killzone (Mở cửa - Bẫy thanh khoản sáng)
    if time(9, 15) <= t <= time(9, 45):
        return "ATO Killzone"
    
    # 2. Morning Manipulation (Thường tạo đỉnh/đáy phiên sáng)
    if time(10, 30) <= t <= time(11, 0):
        return "Morning Trap"
        
    # 3. Afternoon Reversal (Đảo chiều chiều hoặc tiếp diễn)
    if time(13, 15) <= t <= time(13, 45):
        return "PM Reversal"

    # 4. ATC Run (Dòng tiền lớn chốt NAV)
    if time(14, 15) <= t <= time(14, 30):
        return "ATC Run"
        
    return None

# ==============================================================================
# 3. SILVER BULLET (SB) - SETUP XÁC SUẤT CAO
# ==============================================================================
def scan_silver_bullet(df: pd.DataFrame):
    """
    Tìm Setup Silver Bullet: 
    Là một FVG hình thành trong khung giờ vàng (10h-11h hoặc 13h30-14h15).
    """
    if df.empty: return None
    
    # Lọc nến trong khung giờ SB (Ví dụ khung 1H hoặc 15m)
    sb_candidates = []
    
    for idx, row in df.iterrows():
        t = idx.time()
        # Định nghĩa khung giờ Silver Bullet cho VNI
        is_am_sb = time(10, 0) <= t <= time(11, 0)
        is_pm_sb = time(13, 30) <= t <= time(14, 15)
        
        if is_am_sb or is_pm_sb:
            # Kiểm tra xem nến này có tạo ra FVG không?
            # (Logic FVG cần 3 nến, ở đây ta kiểm tra nến giữa i)
            # Ta cần truy cập lại DataFrame gốc theo vị trí integer để check nến trước/sau
            pass 
            # (Lưu ý: Logic check FVG đã có trong smc_core, 
            # ở đây ta chỉ đánh dấu khung thời gian để tô màu biểu đồ)
            sb_candidates.append(idx)
            
    return sb_candidates