# src/config.py
import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

def get_config(key, default=""):
    # ... (giữ nguyên logic hàm get_config của bạn) ...
    try:
        if key in st.secrets:
            return st.secrets[key]
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return os.getenv(key, default)

CACHE_FOLDER = "data_cache"
SKIP_SYMBOLS = {"VNINDEX", "HNXINDEX", "UPCOMINDEX"}

# --- THÊM 2 DÒNG NÀY VÀO CUỐI FILE ---
TELEGRAM_TOKEN = get_config("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = get_config("TELEGRAM_CHAT_ID")

from datetime import datetime
try:
    from zoneinfo import ZoneInfo
    _VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
except Exception:
    _VN_TZ = None

def now_vn():
    """
    Return current datetime in Vietnam timezone (GMT+7).
    Safe fallback if zoneinfo is unavailable.
    """
    if _VN_TZ is not None:
        return datetime.now(tz=_VN_TZ)
    return datetime.now()

# --- Telegram killzone settings (VN time) ---
TELEGRAM_KILLZONE_ONLY = False
TELEGRAM_ALERT_SCORE_MIN = 2.5   # bạn chỉnh tuỳ khẩu vị
KILLZONE_WINDOWS = ["10:45-11:30", "14:10-15:00"]

