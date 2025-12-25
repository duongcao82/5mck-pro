import pandas as pd
import pandas as pd
from datetime import datetime, timedelta

# 1. MAPPING DANH SÁCH NGÀNH TRONG VN100
SECTOR_MAP = {
    # Ngân hàng
    "VCB": "Ngân hàng", "BID": "Ngân hàng", "CTG": "Ngân hàng", "TCB": "Ngân hàng",
    "MBB": "Ngân hàng", "VPB": "Ngân hàng", "ACB": "Ngân hàng", "HDB": "Ngân hàng",
    "VIB": "Ngân hàng", "STB": "Ngân hàng", "SHB": "Ngân hàng", "TPB": "Ngân hàng",
    "MSB": "Ngân hàng", "OCB": "Ngân hàng", "EIB": "Ngân hàng", "LPB": "Ngân hàng",
    # Bất động sản
    "VIC": "Bất động sản", "VHM": "Bất động sản", "VRE": "Bất động sản", "BCM": "Bất động sản",
    "KBC": "Bất động sản", "KDH": "Bất động sản", "NLG": "Bất động sản", "PDR": "Bất động sản",
    "DXG": "Bất động sản", "DIG": "Bất động sản", "NVL": "Bất động sản", "CEO": "Bất động sản",
    "HDC": "Bất động sản", "IJC": "Bất động sản", "PDR": "Bất động sản",
    # Thép & Vật liệu
    "HPG": "Thép", "HSG": "Thép", "NKG": "Thép", "HT1": "Vật liệu",
    # Chứng khoán
    "SSI": "Chứng khoán", "VND": "Chứng khoán", "VCI": "Chứng khoán", "HCM": "Chứng khoán",
    "FTS": "Chứng khoán", "BSI": "Chứng khoán", "VIX": "Chứng khoán", "VDS": "Chứng khoán",
    # Công nghệ & Bán lẻ
    "FPT": "Công nghệ", "CMG": "Công nghệ", "MWG": "Bán lẻ", "PNJ": "Bán lẻ", 
    "FRT": "Bán lẻ", "DGW": "Bán lẻ", "VTP": "Bán lẻ",
    # Dầu khí & Năng lượng
    "GAS": "Dầu khí", "PLX": "Dầu khí", "PVD": "Dầu khí", "PVS": "Dầu khí", "PVT": "Dầu khí",
    "POW": "Điện lực", "REE": "Điện lực", "PC1": "Điện lực", "GEG": "Điện lực",
    # Hóa chất & Phân bón
    "DGC": "Hóa chất", "DCM": "Hóa chất", "DPM": "Hóa chất", "GVR": "Hóa chất", "PHR": "Hóa chất",
    # Sản xuất & Xuất khẩu
    "VNM": "Thực phẩm", "MSN": "Thực phẩm", "SAB": "Thực phẩm", "DBC": "Thực phẩm",
    "GMD": "Logistics", "HAH": "Logistics", "VHC": "Thủy sản", "ANV": "Thủy sản"
}

def analyze_performance_by_sector(csv_file="backtest_vn100_final.csv"):
    try:
        # Tải dữ liệu từ kết quả Backtest
        df = pd.read_csv(csv_file)
        
        # Gán ngành cho các mã (Nếu không có trong Map thì để là 'Khác')
        df['Ngành'] = df['symbol'].map(SECTOR_MAP).fillna("Khác")
        
        # Tính Vốn đầu tư cho mỗi lệnh để tính ROI Ngành chính xác
        df['Investment'] = df['entry_price'] * df['quantity']
        
        # Nhóm theo ngành để tính toán
        stats = df.groupby('Ngành').agg(
            Số_Lệnh=('Ngành', 'count'),
            Win_Rate=('result', lambda x: (x == 'TP').mean() * 100),
            Tổng_Lãi_Ròng=('pnl', 'sum'),
            Vốn_Luân_Chuyển=('Investment', 'sum')
        ).reset_index()
        
        # ROI Ngành = Tổng lãi ròng / Tổng vốn luân chuyển của ngành đó
        stats['ROI_Ngành (%)'] = (stats['Tổng_Lãi_Ròng'] / stats['Vốn_Luân_Chuyển']) * 100
        
        # Sắp xếp theo ROI cao nhất
        stats = stats.sort_values(by='ROI_Ngành (%)', ascending=False)
        
        # Định dạng hiển thị
        print("\n" + "="*60)
        print("📊 BÁO CÁO HIỆU QUẢ THEO NHÓM NGÀNH (SMC SNIPER)")
        print("="*60)
        
        # Format tiền tệ
        stats['Tổng_Lãi_Ròng'] = stats['Tổng_Lãi_Ròng'].apply(lambda x: f"{x:,.0f} đ")
        
        print(stats[['Ngành', 'Số_Lệnh', 'Win_Rate', 'Tổng_Lãi_Ròng', 'ROI_Ngành (%)']].to_string(index=False))
        print("="*60)
        
    except FileNotFoundError:
        print("❌ Lỗi: Không tìm thấy file 'backtest_vn100_final.csv'.")
        print("👉 Vui lòng chạy file 'backtest.py' trước để tạo dữ liệu.")

if __name__ == "__main__":
    analyze_performance_by_sector()
def update_dynamic_sector_rankings(csv_file="backtest_vn100_final.csv", lookback_days=20):
    """
    Tự động tính toán SECTOR_RANKING dựa trên hiệu suất thực tế X ngày qua.
    """
    try:
        df = pd.read_csv(csv_file)
        df['exit_time'] = pd.to_datetime(df['exit_time'])
        
        # 1. Lọc dữ liệu trong thời gian lookback
        cutoff_date = datetime.now() - timedelta(days=lookback_days)
        recent_df = df[df['exit_time'] >= cutoff_date].copy()
        
        if recent_df.empty:
            print("⚠️ Chưa có đủ dữ liệu gần đây, sử dụng bảng xếp hạng mặc định (1.0).")
            return {sector: 1.0 for sector in set(SECTOR_MAP.values())}

        # 2. Tính ROI thực tế từng ngành
        recent_df['Ngành'] = recent_df['symbol'].map(SECTOR_MAP).fillna("Khác")
        recent_df['Investment'] = recent_df['entry_price'] * recent_df['quantity']
        
        sector_stats = recent_df.groupby('Ngành').agg(
            pnl=('pnl', 'sum'),
            capital=('Investment', 'sum')
        )
        sector_stats['roi'] = (sector_stats['pnl'] / sector_stats['capital']) * 100

        # 3. Chuyển đổi ROI thành Multiplier (Hệ số đi tiền)
        # Nguyên tắc: ROI dương > 5% (1.5x), ROI dương (1.2x), ROI âm (0.5x)
        rankings = {}
        for sector, row in sector_stats.iterrows():
            if row['roi'] >= 5:
                rankings[sector] = 1.5  # Ngành siêu mạnh (Vốn 1 tỷ tập trung ở đây)
            elif row['roi'] > 0:
                rankings[sector] = 1.2  # Ngành tiềm năng
            elif row['roi'] > -3:
                rankings[sector] = 0.8  # Ngành đang điều chỉnh nhẹ
            else:
                rankings[sector] = 0.5  # Ngành yếu, giảm rủi ro tối đa

        print(f"✅ Đã cập nhật Sector Rotation cho {len(rankings)} ngành.")
        return rankings

    except Exception as e:
        print(f"❌ Không thể cập nhật Ranking: {e}")
        return {}