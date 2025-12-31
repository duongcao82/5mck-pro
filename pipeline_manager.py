# src/pipeline_manager.py
import os
import time
import math
import pandas as pd
import concurrent.futures
from datetime import datetime, timedelta
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
        # Fallback nếu không có thư viện (để tránh lỗi import)
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
    """Fetcher: Tải dữ liệu hỗ trợ nhiều khung thời gian"""
    def _vn_call(self, ticker: str, **kwargs) -> pd.DataFrame:
        try:
            from vnstock_data import Quote
        except Exception:
            return pd.DataFrame()
            
        start = kwargs.get('start')
        end = kwargs.get('end')
        interval = kwargs.get('interval', '1D') 
        
        # Thử lần lượt các nguồn: TCBS -> VND -> VCI
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
        
        # Merge với dữ liệu cũ để giữ lịch sử nếu cần
        if os.path.exists(file_path):
            try:
                df_old = pd.read_parquet(file_path)
                data = pd.concat([df_old, data])
                data = data[~data.index.duplicated(keep='last')]
                data = data.sort_index()
            except: pass
            
        data.to_parquet(file_path)

# ==============================================================================
# 4. HÀM CHECK CACHE (TỐI ƯU HÓA)
# ==============================================================================
def filter_uptodate_tickers(tickers, interval, target_end_date_str):
    """
    Loại bỏ các mã đã có dữ liệu mới nhất trong Cache.
    Trả về: Danh sách mã CẦN tải.
    """
    needed = []
    skipped = 0
    target_date = pd.to_datetime(target_end_date_str).date()
    
    for sym in tickers:
        file_path = os.path.join(CACHE_DIR, f"{sym}_{interval}.parquet")
        if not os.path.exists(file_path):
            needed.append(sym)
            continue
            
        try:
            # Đọc nhanh file parquet (chỉ lấy index để check ngày)
            df = pd.read_parquet(file_path)
            if df.empty:
                needed.append(sym)
                continue
                
            last_date_in_cache = df.index[-1].date()
            
            # Nếu ngày cuối trong cache >= ngày hiện tại -> Skip
            if last_date_in_cache >= target_date:
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
    """
    Quy trình:
    1. Check Cache D1 -> Chỉ tải mã thiếu/cũ.
    2. Lọc thanh khoản trên toàn bộ data (cũ + mới).
    3. Check Cache Intraday -> Chỉ tải mã thiếu/cũ cho list đạt chuẩn.
    """
    if not HAS_PIPELINE:
        return "⚠️ Lỗi: Chưa cài đặt thư viện 'vnstock_data'."
        
    try:
        fetcher = AppCacheFetcher()
        exporter = ParquetCacheExporter()
        scheduler = Scheduler(fetcher=fetcher, exporter=exporter, max_workers=10)
        
        now = now_vn() 
        end_date = now.strftime('%Y-%m-%d')
        start_date_d1 = (now - timedelta(days=days_back)).strftime('%Y-%m-%d')
        start_date_intra = (now - timedelta(days=4)).strftime('%Y-%m-%d')

        # --- BƯỚC 1: TẢI D1 (CÓ CHECK CACHE) ---
        d1_needed, d1_skipped = filter_uptodate_tickers(tickers_list, '1D', end_date)
        
        if d1_needed:
            print(f"🔄 [1/3] Cần tải D1 cho {len(d1_needed)} mã (Skip {d1_skipped} mã đã mới)...")
            scheduler.run(
                tickers=d1_needed,
                fetcher_kwargs={'start': start_date_d1, 'end': end_date, 'interval': '1D'},
                exporter_kwargs={'output_dir': CACHE_DIR, 'interval': '1D'}
            )
        else:
            print(f"✅ [1/3] D1 đã mới nhất ({len(tickers_list)} mã). Bỏ qua tải.")

        # --- BƯỚC 2: LỌC THANH KHOẢN (Check trên file Cache) ---
        valid_tickers = []
        min_price = 5.0; min_vol = 50_000; min_val = 5_000_000

        print("🔍 [2/3] Đang lọc thanh khoản từ Cache...")
        for sym in tickers_list:
            try:
                path = os.path.join(CACHE_DIR, f"{sym}_1D.parquet")
                if os.path.exists(path):
                    df = pd.read_parquet(path)
                    if len(df) > 5:
                        last = df.iloc[-1]
                        close = float(last['Close'])
                        vol = float(df['Volume'].tail(5).mean())
                        if close > min_price and vol > min_vol and (close * vol) > min_val:
                            valid_tickers.append(sym)
            except: continue
        
        print(f"✅ Đã lọc: {len(valid_tickers)}/{len(tickers_list)} mã đạt chuẩn.")

        # --- BƯỚC 3: TẢI INTRADAY (BATCHING + CHECK CACHE) ---
        if valid_tickers:
            # Lọc xem mã nào thực sự cần tải Intraday
            h1_needed, h1_skipped = filter_uptodate_tickers(valid_tickers, '1H', end_date)
            # Lưu ý: 15m thường cần tải cùng lúc với 1H cho đồng bộ
            
            final_intra_needed = h1_needed
            
            if final_intra_needed:
                BATCH_SIZE = 40
                total = len(final_intra_needed)
                num_batches = math.ceil(total / BATCH_SIZE)
                
                print(f"🔄 [3/3] Cần tải Intraday cho {total} mã (Skip {len(valid_tickers) - total} mã)...")

                for i in range(0, total, BATCH_SIZE):
                    batch = final_intra_needed[i : i + BATCH_SIZE]
                    print(f"   📦 Batch {(i//BATCH_SIZE)+1}/{num_batches}: Tải {len(batch)} mã...")
                    
                    scheduler.run(batch, 
                        {'start': start_date_intra, 'end': end_date, 'interval': '1H'}, 
                        {'output_dir': CACHE_DIR, 'interval': '1H'})
                    
                    scheduler.run(batch, 
                        {'start': start_date_intra, 'end': end_date, 'interval': '15m'}, 
                        {'output_dir': CACHE_DIR, 'interval': '15m'})

                    if i + BATCH_SIZE < total:
                        print("   zzz Nghỉ 3s...")
                        time.sleep(3)
            else:
                print("✅ [3/3] Intraday đã mới nhất. Bỏ qua tải.")
        else:
            print("⚠️ Không có mã nào đạt chuẩn thanh khoản.")

        return f"✅ Hoàn tất! (D1 mới: {len(d1_needed)}, Intra mới: {len(final_intra_needed)})"
        
    except Exception as e:
        return f"❌ Lỗi Runtime: {str(e)}"

def run_universe_pipeline(universe_list, days=20):
    return run_bulk_update(universe_list, days_back=days)