# src/universe.py
import pandas as pd
import concurrent.futures
from data import load_data_with_cache
from pipeline_manager import run_universe_pipeline

# Import thư viện
try:
    from vnstock_data import Listing
    HAS_VNSTOCK_LIB = True
except ImportError:
    HAS_VNSTOCK_LIB = False

# ==============================================================================
# DANH SÁCH DỰ PHÒNG (FALLBACK) - 100 MÃ CỦA BẠN
# ==============================================================================
RAW_TICKERS_STR = """ACB,ANV,BCM,BID,BMP,BSI,BVH,BWE,CII,CMG,CTD,CTG,CTR,CTS,DBC,DCM,DGC,DGW,DIG,DPM,DSE,DXG,DXS,EIB,EVF,FPT,FRT,FTS,GAS,GEE,GEX,GMD,GVR,HAG,HCM,HDB,HDC,HDG,HHV,HPG,HSG,HT1,IMP,KBC,KDC,KDH,KOS,LPB,MBB,MSB,MSN,MWG,NAB,NKG,NLG,NT2,OCB,PAN,PC1,PDR,PHR,PLX,PNJ,POW,PPC,PTB,PVD,PVT,REE,SAB,SBT,SCS,SHB,SIP,SJS,SSB,SSI,STB,SZC,TCB,TCH,TLG,TPB,VCB,VCG,VCI,VGC,VHC,VHM,VIB,VIC,VIX,VJC,VND,VNM,VPB,VPI,VRE,VSC,VTP"""
FALLBACK_LIST = [x.strip() for x in RAW_TICKERS_STR.replace("\n", "").split(",") if x.strip()]

def check_liquidity_worker(symbol, min_price, min_vol_avg_5, min_turnover):
    """Worker kiểm tra thanh khoản"""
    try:
        # Load Cache 20 ngày
        df = load_data_with_cache(symbol, days_to_load=20, timeframe="1D")
        if df is None or len(df) < 5: return None
        
        if "Close" not in df: df["Close"] = df.get("close")
        if "Volume" not in df: df["Volume"] = df.get("volume")

        last_row = df.iloc[-1]
        close = float(last_row["Close"])
        vol_avg_5 = df["Volume"].tail(5).mean()
        
        # Turnover (đơn vị gốc trong data là nghìn đồng -> so với 10 triệu)
        turnover = close * vol_avg_5

        if close <= min_price: return None
        if vol_avg_5 < min_vol_avg_5: return None
        if turnover < min_turnover: return None

        return symbol
    except:
        return None

def get_vnallshare_universe(days=20, min_price=5.0, min_vol_avg_5=50_000, min_turnover=10_000_000):
    """
    Logic 3 Tầng: VNALLSHARE -> VN100 -> Danh sách cứng
    """
    symbols = []
    source_used = "Fallback"

    if HAS_VNSTOCK_LIB:
        # --- TẦNG 1: THỬ VNALLSHARE (ƯU TIÊN CAO NHẤT) ---
        try:
            listing = Listing(source='vci')
            symbols = listing.symbols_by_group("VNALLSHARE").tolist()
            if symbols and len(symbols) > 100:
                source_used = "API VCI (VNALLSHARE)"
        except Exception:
            pass # Lặng lẽ bỏ qua để xuống tầng 2
        
        # --- TẦNG 2: NẾU TẦNG 1 THẤT BẠI -> THỬ VN100 ---
        if not symbols:
            try:
                listing = Listing(source='vci')
                symbols = listing.symbols_by_group("VN100").tolist()
                if symbols and len(symbols) > 50:
                    source_used = "API VCI (VN100)"
            except Exception:
                pass # Lặng lẽ bỏ qua để xuống tầng 3

    # --- TẦNG 3: NẾU CẢ 2 API ĐỀU CHẾT -> DÙNG DANH SÁCH CỨNG ---
    if not symbols:
        symbols = FALLBACK_LIST
        source_used = "Fixed List (Manual)"

    # Lọc mã rác
    symbols = [s for s in symbols if len(s) == 3]

    print(f"[Universe] 📋 Nguồn: {source_used} ({len(symbols)} mã).")
    print(f"[Universe] ⚡ Kích hoạt Pipeline Update (D1)...")
    run_universe_pipeline(symbols, days=days)

    print(f"[Universe] 🔍 Đang lọc thanh khoản...")
    valid_universe = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futs = {executor.submit(check_liquidity_worker, s, min_price, min_vol_avg_5, min_turnover): s for s in symbols}
        count_done = 0
        for fut in concurrent.futures.as_completed(futs):
            res = fut.result()
            if res: valid_universe.append(res)
            count_done += 1
            if count_done % 50 == 0: print(f"... Checked {count_done}/{len(symbols)}")

    print(f"[Universe] ✅ Kết quả: {len(valid_universe)}/{len(symbols)} mã đạt chuẩn.")
    
    if not valid_universe:
        return FALLBACK_LIST
    return sorted(valid_universe)