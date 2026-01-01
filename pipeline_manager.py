# src/pipeline_manager.py
import os
import time
import math
import pandas as pd
import concurrent.futures
from datetime import datetime, timedelta, date
from config import now_vn

# --- ĐỊNH NGHĨA ĐƯỜNG DẪN TUYỆT ĐỐI ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
CACHE_DIR = os.path.join(BASE_DIR, "data_cache")      

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# --- INIT ---
HAS_PIPELINE = False
VNFetcher = object 
Exporter = object

try:
    from vnstock_pipeline.template.vnstock import VNFetcher
    from vnstock_pipeline.core.exporter import Exporter
    HAS_PIPELINE = True
except ImportError:
    try:
        from vnstock_pipeline.core.fetcher import VNFetcher
        from vnstock_pipeline.core.exporter import Exporter
        HAS_PIPELINE = True
    except:
        pass

# ==============================================================================
# 2. CUSTOM SCHEDULER
# ==============================================================================
class SimpleScheduler:
    def __init__(self, fetcher, exporter, max_workers=5):
        self.fetcher = fetcher
        self.exporter = exporter
        self.max_workers = max_workers

    def _process_task(self, ticker, f_kwargs, e_kwargs):
        try:
            df = self.fetcher._vn_call(ticker, **f_kwargs)
            if df is not None and not df.empty:
                self.exporter.export(df, ticker, **e_kwargs)
            return True, f"{ticker}: OK"
        except Exception as e:
            return False, f"{ticker}: {str(e)}"

    def run(self, tickers, fetcher_kwargs, exporter_kwargs):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_ticker = {
                executor.submit(self._process_task, t, fetcher_kwargs, exporter_kwargs): t 
                for t in tickers
            }
            for future in concurrent.futures.as_completed(future_to_ticker):
                pass 
        return "Xong"

Scheduler = SimpleScheduler

# ==============================================================================
# 3. CẤU HÌNH PIPELINE
# ==============================================================================

class AppCacheFetcher(VNFetcher):
    def _vn_call(self, ticker: str, **kwargs) -> pd.DataFrame:
        try:
            from vnstock_data import Quote
        except Exception:
            return pd.DataFrame()
            
        start = kwargs.get('start')
        end = kwargs.get('end')
        interval = kwargs.get('interval', '1D') 
        
        sources = ['tcbs', 'vnd', 'vci']
        for src in sources:
            try:
                quote = Quote(source=src, symbol=ticker)
                df = quote.history(start=start, end=end, interval=interval)
                if df is not None and not df.empty:
                    df = df.rename(columns={
                        'time': 'Date', 'open': 'Open', 'high': 'High', 
                        'low': 'Low', 'close': 'Close', 'volume': 'Volume'
                    })
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                        df = df.set_index('Date')
                    return df
            except:
                continue
        return pd.DataFrame()

class ParquetCacheExporter(Exporter):
    def export(self, data: pd.DataFrame, ticker: str, **kwargs):
        output_dir = kwargs.get('output_dir', CACHE_DIR)
        interval = kwargs.get('interval', '1D') 
        file_path = os.path.join(output_dir, f"{ticker}_{interval}.parquet")
        
        if os.path.exists(file_path):
            try:
                df_old = pd.read_parquet(file_path)
                data = pd.concat([df_old, data])
                data = data[~data.index.duplicated(keep='last')]
                data = data.sort_index()
            except: pass
            
        data.to_parquet(file_path)

# ==============================================================================
# 4. LOGIC TÍNH NGÀY GIAO DỊCH THÔNG MINH (HỖ TRỢ KHOẢNG THỜI GIAN)
# ==============================================================================
def get_last_trading_date():
    """
    Tìm ngày giao dịch gần nhất (Trừ lễ tết, cuối tuần).
    """
    candidate = now_vn()
    
    # -----------------------------------------------------------
    # CẤU HÌNH LỊCH NGHỈ LỄ (CẬP NHẬT TẠI ĐÂY)
    # -----------------------------------------------------------
    
    # 1. Ngày lễ cố định (Dương lịch) - Chỉ cần MM-DD
    FIXED_HOLIDAYS_MMDD = ["01-01", "04-30", "05-01", "09-02"]
    
    # 2. Khoảng thời gian nghỉ dài (Tết, Giỗ tổ...) - Format: ("YYYY-MM-DD", "YYYY-MM-DD")
    # Ví dụ: Tết Ất Tỵ nghỉ từ 25/01 đến hết 02/02/2025
    HOLIDAY_RANGES = [
        ("2026-01-01", "2026-01-04"), # Tết Dương
        ("2026-02-14", "2026-02-22"), # Tết Âm
        ("2026-04-25", "2026-04-27"), # Giỗ tổ
        ("2026-04-30", "2026-05-03"), # 30/4
        ("2026-08-29", "2026-09-02"), # 2/9
    ]
    # -----------------------------------------------------------

    while True:
        candidate_str = candidate.strftime('%Y-%m-%d')
        mm_dd = candidate.strftime('%m-%d')

        # 1. Check Giờ Giao Dịch (Nếu là Hôm nay)
        if candidate.date() == now_vn().date():
            if candidate.hour < 15 or (candidate.hour == 15 and candidate.minute < 15):
                candidate -= timedelta(days=1)
                continue

        # 2. Check Cuối Tuần (T7, CN)
        if candidate.weekday() >= 5:
            candidate -= timedelta(days=1)
            continue

        # 3. Check Ngày Lễ Cố Định (01/01, 30/04...)
        if mm_dd in FIXED_HOLIDAYS_MMDD:
            candidate -= timedelta(days=1)
            continue
            
        # 4. Check Khoảng Nghỉ Lễ (Range)
        is_in_range = False
        for start_str, end_str in HOLIDAY_RANGES:
            if start_str <= candidate_str <= end_str:
                is_in_range = True
                break
        
        if is_in_range:
            candidate -= timedelta(days=1)
            continue

        # Nếu không dính điều kiện nào -> Đây là ngày giao dịch
        return candidate.date()

def filter_uptodate_tickers(tickers, interval, target_date_obj):
    """Lọc bỏ mã đã có dữ liệu mới nhất"""
    needed = []
    skipped = 0
    
    for sym in tickers:
        file_path = os.path.join(CACHE_DIR, f"{sym}_{interval}.parquet")
        if not os.path.exists(file_path):
            needed.append(sym)
            continue
            
        try:
            df = pd.read_parquet(file_path)
            if df.empty:
                needed.append(sym)
                continue
            
            last_date_in_cache = df.index[-1].date()
            if last_date_in_cache >= target_date_obj:
                skipped += 1
            else:
                needed.append(sym)
        except:
            needed.append(sym)
            
    return needed, skipped

# ==============================================================================
# 5. HÀM CHẠY CHÍNH
# ==============================================================================

def run_bulk_update(tickers_list, days_back=200):
    if not HAS_PIPELINE:
        return "⚠️ Lỗi: Chưa cài đặt thư viện 'vnstock_data'."
        
    try:
        fetcher = AppCacheFetcher()
        exporter = ParquetCacheExporter()
        scheduler = Scheduler(fetcher=fetcher, exporter=exporter, max_workers=10)
        
        # [QUAN TRỌNG] Lấy ngày giao dịch CHUẨN
        target_date = get_last_trading_date()
        target_date_str = target_date.strftime('%Y-%m-%d')
        
        now = now_vn()
        end_date_api = now.strftime('%Y-%m-%d') 
        start_date_d1 = (now - timedelta(days=days_back)).strftime('%Y-%m-%d')
        start_date_intra = (now - timedelta(days=4)).strftime('%Y-%m-%d')

        print(f"📅 Ngày giao dịch mục tiêu: {target_date_str} (Hôm nay: {now.strftime('%d/%m %H:%M')})")

        # --- BƯỚC 1: TẢI D1 (CHECK CACHE) ---
        d1_needed, d1_skipped = filter_uptodate_tickers(tickers_list, '1D', target_date)
        
        if d1_needed:
            print(f"🔄 [1/3] Cần tải D1 cho {len(d1_needed)} mã (Skip {d1_skipped} mã đã đủ)...")
            scheduler.run(
                tickers=d1_needed,
                fetcher_kwargs={'start': start_date_d1, 'end': end_date_api, 'interval': '1D'},
                exporter_kwargs={'output_dir': CACHE_DIR, 'interval': '1D'}
            )
        else:
            print(f"✅ [1/3] D1 đã đủ dữ liệu đến {target_date_str}. Bỏ qua tải.")

        # --- BƯỚC 2: LỌC THANH KHOẢN ---
        valid_tickers = []
        min_price = 5.0        
        min_vol = 50_000       
        # Turnover 10 Tỷ (đơn vị nghìn đồng)
        min_val = 10_000_000   

        print("🔍 [2/3] Check thanh khoản từ Cache...")
        for sym in tickers_list:
            try:
                path = os.path.join(CACHE_DIR, f"{sym}_1D.parquet")
                if os.path.exists(path):
                    df = pd.read_parquet(path)
                    if len(df) > 5:
                        last = df.iloc[-1]
                        
                        close = float(last['Close'])
                        vol_avg = float(df['Volume'].tail(5).mean())
                        turnover = close * vol_avg
                        
                        if close > min_price and vol_avg > min_vol and turnover > min_val:
                            valid_tickers.append(sym)
            except: continue
        
        print(f"✅ Đã lọc: {len(valid_tickers)}/{len(tickers_list)} mã đạt chuẩn > 10 Tỷ.")

        # --- BƯỚC 3: TẢI INTRADAY (BATCHING + CHECK CACHE) ---
        if valid_tickers:
            h1_needed, h1_skipped = filter_uptodate_tickers(valid_tickers, '1H', target_date)
            
            if h1_needed:
                BATCH_SIZE = 20 
                total = len(h1_needed)
                num_batches = math.ceil(total / BATCH_SIZE)
                
                print(f"🔄 [3/3] Cần tải Intraday cho {total} mã (Skip {len(valid_tickers) - total} mã)...")

                for i in range(0, total, BATCH_SIZE):
                    batch = h1_needed[i : i + BATCH_SIZE]
                    print(f"   📦 Batch {(i//BATCH_SIZE)+1}/{num_batches}: Tải {len(batch)} mã...")
                    
                    scheduler.run(batch, 
                        {'start': start_date_intra, 'end': end_date_api, 'interval': '1H'}, 
                        {'output_dir': CACHE_DIR, 'interval': '1H'})
                    
                    scheduler.run(batch, 
                        {'start': start_date_intra, 'end': end_date_api, 'interval': '15m'}, 
                        {'output_dir': CACHE_DIR, 'interval': '15m'})

                    if i + BATCH_SIZE < total:
                        print("   zzz Nghỉ 10s...") 
                        time.sleep(10)
            else:
                print(f"✅ [3/3] Intraday đã đủ dữ liệu đến {target_date_str}. Bỏ qua tải.")
        else:
            print("⚠️ Không có mã nào đạt chuẩn thanh khoản.")

        return f"✅ Hoàn tất! (D1 mới: {len(d1_needed)}, Intra mới: {len(h1_needed)})"
        
    except Exception as e:
        return f"❌ Lỗi Runtime: {str(e)}"

def run_universe_pipeline(universe_list, days=20):
    return run_bulk_update(universe_list, days_back=days)