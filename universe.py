# universe.py
import pandas as pd
import concurrent.futures
from data import load_data_with_cache
from pipeline_manager import run_universe_pipeline

# ==============================================================================
# DANH SÁCH CỐ ĐỊNH (HARDCODED UNIVERSE)
# Giúp tránh lỗi API Listing khi mạng chập chờn
# ==============================================================================
RAW_TICKERS_STR = """AAA,AAM,ABR,ABS,ABT,ACB,ACC,ACL,ADS,AGG,AGR,ANV,APG,APH,ASM,ASP,AST,BAF,BCE,BCG,BCM,BFC,BIC,BID,BKG,BMC,BMI,BMP,BRC,BSI,BTP,BVH,BWE,C32,CCL,CDC,CII,CLC,CLL,CMG,CMX,CNG,CRC,CRE,CSM,CSV,CTD,CTF,CTG,CTI,CTR,CTS,D2D,DAH,DBC,DBD,DBT,DC4,DCL,DCM,DGC,DGW,DHA,DHC,DHM,DIG,DPG,DPM,DPR,DRC,DRL,DSE,DSN,DTA,DVP,DXG,DXS,EIB,ELC,EVE,EVF,FCM,FCN,FIR,FIT,FMC,FPT,FRT,FTS,GAS,GDT,GEX,GIL,GMD,GMH,GSP,GTA,GVR,HAG,HAH,HAP,HAR,HAX,HCD,HCM,HDB,HDC,HDG,HHP,HHS,HHV,HID,HII,HMC,HPG,HPX,HQC,HSG,HSL,HT1,HTG,HTI,HTN,HTV,HUB,HVH,ICT,IDI,IJC,IMP,ITC,ITD,JVC,KBC,KDC,KDH,KHG,KHP,KMR,KOS,KSB,LAF,LBM,LCG,LHG,LIX,LPB,LSS,MBB,MCM,MCP,MHC,MIG,MSB,MSH,MSN,MWG,NAB,NAF,NBB,NCT,NHA,NHH,NKG,NLG,NNC,NO1,NSC,NT2,NTL,OCB,OGC,ORS,PAC,PAN,PC1,PDR,PET,PGC,PHC,PHR,PIT,PLP,PLX,PNJ,POW,PPC,PTB,PTC,PTL,PVD,PVP,PVT,QCG,RAL,REE,SAB,SAM,SAV,SBG,SBT,SCR,SCS,SFC,SFG,SFI,SGN,SGR,SGT,SHB,SHI,SIP,SJD,SJS,SKG,SMB,SSB,SSI,ST8,STB,STK,SVT,SZC,SZL,TCB,TCD,TCH,TCI,TCL,TCM,TCO,TCT,TDC,TDG,TDP,TEG,THG,TIP,TLD,TLG,TLH,TMT,TNH,TNI,TNT,TPB,TRC,TTA,TTF,TV2,TVS,TVT,TYA,UIC,VCA,VCB,VCG,VCI,VDS,VFG,VGC,VHC,VHM,VIB,VIC,VIP,VIX,VJC,VMD,VND,VNL,VNM,VNS,VOS,VPB,VPG,VPH,VPI,VRC,VRE,VSC,VTB,VTO,VTP,YEG"""

# Chuyển chuỗi thành list và loại bỏ khoảng trắng thừa
FIXED_HOSE_LIST = [x.strip() for x in RAW_TICKERS_STR.replace("\n", "").split(",") if x.strip()]

# Danh sách dự phòng (dù ít khi dùng tới vì đã hardcode ở trên)
FALLBACK_VN100 = FIXED_HOSE_LIST[:100] 


def check_liquidity_worker(symbol, min_price, min_vol_avg_5, min_turnover):
    """
    Worker kiểm tra từng mã:
    - Đọc dữ liệu từ Cache (vừa được Pipeline cập nhật).
    - Tính toán Price, Vol, Turnover.
    """
    try:
        # Load từ Cache (chỉ 20 ngày, cực nhanh)
        df = load_data_with_cache(symbol, days_to_load=20, timeframe="1D")

        # Kiểm tra: Cần ít nhất 5 phiên để tính trung bình
        if df is None or len(df) < 5: 
            return None

        # Chuẩn hóa cột
        if "Close" not in df: df["Close"] = df["close"]
        if "Volume" not in df: df["Volume"] = df["volume"]

        last_row = df.iloc[-1]
        close = float(last_row["Close"])
        
        # Tính Vol trung bình 5 phiên gần nhất
        vol_avg_5 = df["Volume"].tail(5).mean()

        # --- LOGIC LỌC ---
        # 1. Giá < min_price (10.000đ) -> Bỏ
        if close <= min_price: 
            return None
        
        # 2. Vol trung bình < min_vol -> Bỏ
        if vol_avg_5 < min_vol_avg_5:
            return None
        
        # 3. Giá trị GD (Turnover) < min_turnover (10 Tỷ) -> Bỏ
        turnover = close * vol_avg_5
        if turnover < min_turnover:
            return None

        return symbol

    except Exception:
        return None


def get_vnallshare_universe(
    days=20, # Load 20 ngày để check Vol TB 5 phiên
    min_price=10,               
    min_vol_avg_5=100_000,      
    min_turnover=10_000_000     
):
    """
    Quy trình Universe Mới:
    1. Lấy danh sách cố định (Hardcoded).
    2. Chạy Pipeline để update dữ liệu mới nhất (D1) cho danh sách này.
    3. Đọc lại Cache để lọc mã đủ điều kiện thanh khoản.
    """
    
    # 1. LẤY DANH SÁCH CỐ ĐỊNH
    raw_symbols = FIXED_HOSE_LIST
    print(f"[Universe] 📋 Sử dụng danh sách cố định: {len(raw_symbols)} mã.")

    # 2. CHẠY PIPELINE UPDATE (Quan trọng: Lấy dữ liệu hôm nay)
    print(f"[Universe] ⚡ Kích hoạt Pipeline Update (D1, {days} days)...")
    
    # Gọi hàm từ pipeline_manager.py để tải dữ liệu song song
    success = run_universe_pipeline(raw_symbols, days=days)
    
    if not success:
        print("[Universe] ⚠️ Pipeline Update có lỗi (có thể do mạng), vẫn tiếp tục lọc trên Cache cũ...")

    # 3. LỌC THANH KHOẢN (Đọc từ Cache)
    print(f"[Universe] 🔍 Đang lọc thanh khoản (Filter)...")
    valid_universe = []
    
    # Sử dụng đa luồng để đọc Cache và lọc cho nhanh
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_symbol = {
            executor.submit(check_liquidity_worker, sym, min_price, min_vol_avg_5, min_turnover): sym 
            for sym in raw_symbols
        }
        
        count_done = 0
        total = len(raw_symbols)
        
        for future in concurrent.futures.as_completed(future_to_symbol):
            result = future.result()
            if result:
                valid_universe.append(result)
            count_done += 1
            
            # Update tiến độ nhẹ nhàng
            if count_done % 50 == 0:
                print(f"[Universe] ... Đã kiểm tra {count_done}/{total} mã")

    print(f"[Universe] ✅ Hoàn tất. Kết quả: {len(valid_universe)}/{total} mã đạt chuẩn > 10 Tỷ.")
    
    # Nếu lọc xong mà không còn mã nào (ví dụ dữ liệu lỗi hết), trả về danh sách gốc nhưng cắt 100 mã đầu
    if not valid_universe:
        print("[Universe] ⚠️ Không tìm thấy mã nào đạt chuẩn, trả về Top 100 mặc định.")
        return FALLBACK_VN100
    
    valid_universe.sort()
    return valid_universe