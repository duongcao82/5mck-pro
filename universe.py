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
# DANH SÁCH 285 MÃ CỐ ĐỊNH (CỦA BẠN)
# ==============================================================================
# HÃY DÁN 285 MÃ CỦA BẠN VÀO GIỮA 3 DẤU NGOẶC KÉP DƯỚI ĐÂY
RAW_TICKERS_STR = """
AAA, AAM, AAS, ABS, ABT, ACB, ACC, ACL, ADG, ADS, AGG, AGM, AGR, AKV, ALT, AMC, AME, AMV, ANV, APC, APG, APH, ASG, ASM, ASP, AST,
BAF, BBC, BCE, BCG, BCM, BFC, BHN, BIC, BID, BKG, BMC, BMI, BMP, BRC, BSI, BTP, BTT, BVH, BWE, 
C32, C47, CAV, CCI, CCL, CDC, CEG, CEO, CHP, CIA, CII, CKG, CLC, CLL, CMG, CMV, CNG, COM, CRC, CRE, CSM, CSV, CTD, CTF, CTG, CTI, CTR, CTS, CVT, D2D, DAG, DAH, DAT, DBC, DBD, DBT, DC4, DCL, DCM, DGC, DGW, DHA, DHC, DHG, DHM, DIG, DLG, DMC, DPG, DPM, DPR, DQC, DRC, DRH, DRL, DSE, DSN, DTA, DTK, DTL, DTT, DVP, DXG, DXS, DXV, EIB, ELC, EVE, EVF, EVG, FCM, FCN, FDC, FIR, FIT, FMC, FPT, FRT, FTS, GAS, GDT, GDU, GEE, GEX, GIL, GMC, GMD, GMX, GSP, GTA, GVR, HAG, HAH, HAP, HAR, HAS, HAX, HBC, HCD, HCM, HDB, HDC, HDG, HHP, HHS, HHV, HID, HII, HMC, HNG, HPG, HQC, HRC, HSG, HSL, HT1, HTI, HTL, HTN, HTV, HU1, HU3, HUB, HVH, HVN, HVX, ICT, IDI, IJC, ILB, IMP, ITA, ITC, ITD, JVC, KBC, KDC, KDH, KHP, KHG, KIP, KMR, KOS, KPF, KSB, L10, L18, LAF, LBM, LCG, LDG, LEC, LGC, LHG, LIX, LM8, LPB, LSS, MBB, MCV, MDG, MHC, MIG, MSB, MSH, MSN, MWG, NAB, NAF, NAG, NBB, NBP, NCT, NHA, NHH, NKG, NLG, NNC, NO1, NT2, NTL, OCB, OGC, ONE, OPC, ORS, PAC, PAN, PC1, PDN, PDR, PET, PGC, PGD, PGI, PHC, PHR, PIT, PJT, PLP, PLX, PME, PNJ, POM, POW, PPC, PSH, PTB, PTC, PTL, PVD, PVT, QBS, QCG, QNS, RAL, RDP, REE, ROS, S4A, SAB, SAM, SBA, SBT, SBV, SC5, SCD, SCR, SCS, SFI, SGN, SGR, SGT, SHA, SHB, SHI, SHP, SIP, SJD, SJS, SKG, SMA, SMB, SMC, SMT, SOT, SPP, SRA, SRC, SRF, SSB, SSI, ST8, STB, STG, STK, SVC, SVD, SVI, SVT, SZC, SZL, TAC, TBC, TCB, TCD, TCH, TCL, TCM, TCO, TCR, TDC, TDG, TDH, TDM, TDP, TDW, TEG, THC, THT, TIP, TIX, TLG, TLH, TMP, TMS, TMT, TN1, TNA, TNC, TNH, TNI, TNT, TPB, TPC, TRA, TRC, TSC, TTA, TTB, TTC, TTF, TV2, TVB, TVC, TVS, TYA, UIC, VAF, VCA, VCB, VCF, VCG, VCI, VDS, VFG, VGC, VHC, VHM, VIB, VIC, VID, VIP, VIX, VJC, VMD, VND, VNE, VNG, VNL, VNM, VNS, VOS, VPB, VPD, VPG, VPH, VPI, VPS, VRC, VRE, VSC, VSH, VSI, VTB, VTO, VTP, YBM, YEG"""
# (LƯU Ý: BẠN CÓ THỂ PASTE ĐÈ DANH SÁCH CHUẨN CỦA BẠN VÀO TRÊN)

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
        
        # Turnover
        turnover = close * vol_avg_5

        if close <= min_price: return None
        if vol_avg_5 < min_vol_avg_5: return None
        if turnover < min_turnover: return None

        return symbol
    except:
        return None

def get_vnallshare_universe(days=20, min_price=5.0, min_vol_avg_5=50_000, min_turnover=10_000_000):
    """
    Logic 2 Tầng: VNALLSHARE -> Danh sách cứng 285 mã
    """
    symbols = []
    source_used = "Fallback (285 mã)"

    if HAS_VNSTOCK_LIB:
        # --- TẦNG 1: THỬ VNALLSHARE (API) ---
        try:
            listing = Listing(source='vci')
            symbols = listing.symbols_by_group("VNALLSHARE").tolist()
            if symbols and len(symbols) > 200: # VNALLSHARE thường > 300 mã
                source_used = "API VCI (VNALLSHARE)"
        except Exception:
            pass # Lỗi thì xuống tầng 2

    # --- TẦNG 2: NẾU API LỖI -> DÙNG DANH SÁCH 285 MÃ ---
    if not symbols:
        symbols = FALLBACK_LIST
        source_used = f"Danh sách cố định ({len(symbols)} mã)"

    # Lọc mã rác
    symbols = [s for s in symbols if len(s) == 3]

    print(f"[Universe] 📋 Nguồn: {source_used}")
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
    
    # Nếu lọc quá gắt ra 0 mã, trả về list gốc
    if not valid_universe:
        return FALLBACK_LIST
    return sorted(valid_universe)