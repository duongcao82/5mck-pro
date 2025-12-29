# seed_data.py
import os
import pandas as pd
from pipeline_manager import run_bulk_update
from universe import get_vnallshare_universe
from data import load_data_with_cache

def seed_cache_for_git():
    print("🚀 BẮT ĐẦU TẠO DỮ LIỆU CACHE ĐỂ ĐẨY GIT...")
    
    # 1. Lấy danh sách mã (VNALLSHARE + VNINDEX)
    try:
        universe = get_vnallshare_universe(days=20)
        universe.append("VNINDEX") # Bắt buộc phải có ông này
        universe = list(set(universe)) # Loại bỏ trùng
        print(f"📦 Tìm thấy {len(universe)} mã cần tải.")
    except Exception as e:
        print(f"❌ Lỗi lấy Universe: {e}")
        return

    # 2. Chạy tải dữ liệu (Sẽ lưu vào folder /data_cache)
    # Tải D1 (365 ngày), 1H (100 ngày), 15m (30 ngày)
    # Lưu ý: Chạy ở local nên cứ để nó chạy từ từ, không sợ timeout
    print("⏳ Đang tải dữ liệu (có thể mất vài phút)...")
    result = run_bulk_update(universe, days_back=365)
    
    print(result)
    print("✅ Đã xong! Kiểm tra folder 'data_cache' xem có file .parquet chưa.")

if __name__ == "__main__":
    seed_cache_for_git()