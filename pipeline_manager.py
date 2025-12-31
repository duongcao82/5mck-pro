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
    # A. Import VNFetcher
    try:
        from vnstock_pipeline.template.vnstock import VNFetcher
    except ImportError:
        try:
            from vnstock_pipeline.core.fetcher import VNFetcher
        except ImportError:
            try:
                from vnstock_pipeline.core.fetcher import Fetcher as VNFetcher
            except ImportError:
                VNFetcher = object 

    # B. Import Exporter
    try:
        from vnstock_pipeline.core.exporter import Exporter
    except ImportError:
        Exporter = object
     
    HAS_PIPELINE = True

except ImportError as e:
    print(f"⚠️ [Pipeline] Thiếu thư viện dữ liệu: {e}")

# ==============================================================================
# 2. CUSTOM SCHEDULER
# ==============================================================================
class SimpleScheduler:
    def __init__(self, fetcher, exporter, max_workers=5, retry_attempts=1, **kwargs):
        self.fetcher = fetcher
        self.exporter = exporter
        self.max_workers = max_workers

    def _process_task(self, ticker, f_kwargs, e_kwargs):
        try:
            # 1. Fetch Data
            df = self.fetcher._vn_call(ticker, **f_kwargs)
            # 2. Export Data
            if df is not None and not df.empty:
                self.exporter.export(df, ticker, **e_kwargs)
            return True, f"{ticker}: OK"
        except Exception as e:
            return False, f"{ticker}: {str(e)}"

    def run(self, tickers, fetcher_kwargs, exporter_kwargs):
        # Hàm này chỉ chạy, không in log tổng để tránh rối khi chạy batch
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
        if not HAS_PIPELINE: return pd.DataFrame()
        try:
            from vnstock_data import Quote
        except Exception:
            return pd.DataFrame()
        start = kwargs.get('start')
        end = kwargs.get('end')
        interval = kwargs.get('interval', '1D') 
        
        # --- LOGIC MỚI: Thử TCBS -> VND -> VCI ---
        try:
            quote = Quote(source='tcbs', symbol=ticker)
            df = quote.history(start=start, end=end, interval=interval)
        except Exception:
            try:
                quote = Quote(source='vnd', symbol=ticker)
                df = quote.history(start=start, end=end, interval=interval)
            except Exception:
                try:
                    quote = Quote(source='vci', symbol=ticker)
                    df = quote.history(start=start, end=end, interval=interval)
                except:
                    return pd.DataFrame()
        
        if df is None or df.empty:
            raise ValueError(f"No data for {ticker}")
            
        df = df.rename(columns={
            'time': 'Date', 'open': 'Open', 'high': 'High', 
            'low': 'Low', 'close': 'Close', 'volume': 'Volume'
        })
        
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
            
        return df

class ParquetCacheExporter(Exporter):
    """Exporter: Lưu file theo format {Symbol}_{Interval}.parquet"""
    def export(self, data: pd.DataFrame, ticker: str, **kwargs):
        output_dir = kwargs.get('output_dir', CACHE_DIR)
        interval = kwargs.get('interval', '1D') 
        
        os.makedirs(output_dir, exist_ok=True)
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
# 4. HÀM CHẠY CHÍNH (SMART PIPELINE + BATCHING)
# ==============================================================================

def run_bulk_update(tickers_list, days_back=200):
    """
    Cập nhật dữ liệu THÔNG MINH (Smart Pipeline):
    1. Tải D1 (Full list) -> Nhanh, ít bị chặn.
    2. Lọc mã thanh khoản.
    3. Tải Intraday (Chỉ mã đạt chuẩn) -> CHIA BATCH để tránh lỗi 429.
    """
    if not HAS_PIPELINE:
        return "⚠️ Lỗi: Chưa cài đặt thư viện 'vnstock_data'."
        
    try:
        fetcher = AppCacheFetcher()
        exporter = ParquetCacheExporter()
        # Dùng max_workers vừa phải để tránh DDOS
        scheduler = Scheduler(fetcher=fetcher, exporter=exporter, max_workers=10)
        
        now = now_vn() 
        end_date = now.strftime('%Y-%m-%d')
        start_date_d1 = (now - timedelta(days=days_back)).strftime('%Y-%m-%d')
        start_date_intra = (now - timedelta(days=4)).strftime('%Y-%m-%d')

        # --- BƯỚC 1: TẢI D1 (DAILY) - Chạy 1 lèo vì D1 nhẹ ---
        print(f"🔄 [1/3] Đang tải D1 cho {len(tickers_list)} mã...")
        scheduler.run(
            tickers=tickers_list,
            fetcher_kwargs={'start': start_date_d1, 'end': end_date, 'interval': '1D'},
            exporter_kwargs={'output_dir': CACHE_DIR, 'interval': '1D'}
        )

        # --- BƯỚC 2: LỌC THANH KHOẢN ---
        valid_tickers = []
        min_price = 5.0        
        min_vol = 50_000       
        min_val = 5_000_000    

        print("🔍 [2/3] Đang lọc thanh khoản...")
        for sym in tickers_list:
            try:
                path = os.path.join(CACHE_DIR, f"{sym}_1D.parquet")
                if os.path.exists(path):
                    df = pd.read_parquet(path)
                    if len(df) > 5:
                        last = df.iloc[-1]
                        close = float(last['Close'])
                        vol = float(df['Volume'].tail(5).mean())
                        turnover = close * vol 
                        
                        if close > min_price and vol > min_vol and turnover > min_val:
                            valid_tickers.append(sym)
            except:
                continue
        
        print(f"✅ Đã lọc: {len(valid_tickers)}/{len(tickers_list)} mã đạt chuẩn.")

        # --- BƯỚC 3: TẢI INTRADAY (BATCHING - CHIA LÔ) ---
        if valid_tickers:
            BATCH_SIZE = 40  # Tải mỗi lần 40 mã
            total_stocks = len(valid_tickers)
            num_batches = math.ceil(total_stocks / BATCH_SIZE)
            
            print(f"🔄 [3/3] Tải Intraday cho {total_stocks} mã (Chia làm {num_batches} Batch)...")

            for i in range(0, total_stocks, BATCH_SIZE):
                batch = valid_tickers[i : i + BATCH_SIZE]
                current_batch_idx = (i // BATCH_SIZE) + 1
                
                print(f"   📦 Batch {current_batch_idx}/{num_batches}: Tải {len(batch)} mã...")
                
                # Tải 1H
                scheduler.run(
                    tickers=batch,
                    fetcher_kwargs={'start': start_date_intra, 'end': end_date, 'interval': '1H'},
                    exporter_kwargs={'output_dir': CACHE_DIR, 'interval': '1H'}
                )
                
                # Tải 15m
                scheduler.run(
                    tickers=batch,
                    fetcher_kwargs={'start': start_date_intra, 'end': end_date, 'interval': '15m'},
                    exporter_kwargs={'output_dir': CACHE_DIR, 'interval': '15m'}
                )

                # QUAN TRỌNG: Nghỉ giữa các Batch để tránh lỗi 429
                if i + BATCH_SIZE < total_stocks:
                    print("   zzz Nghỉ 5s để tránh nghẽn mạng...")
                    time.sleep(10) 

        else:
            print("⚠️ Không có mã nào đạt chuẩn thanh khoản.")

        return f"✅ Hoàn tất! (D1: {len(tickers_list)} mã, Valid: {len(valid_tickers)} mã)"
        
    except Exception as e:
        return f"❌ Lỗi Runtime: {str(e)}"

# Hàm này giữ nguyên để Universe gọi
def run_universe_pipeline(universe_list, days=20):
    """
    Tối ưu hóa quy trình cập nhật Universe:
    1. Chỉ lấy D1 (nhẹ) để kiểm tra thanh khoản.
    2. Dùng ThreadPool để chạy nhanh trên 20 workers.
    """
    from data import load_data_with_cache
    import concurrent.futures

    print(f"⚡ Pipeline: Đang quét {len(universe_list)} mã (D1)...")
    
    def _update_worker(sym):
        try:
            load_data_with_cache(sym, days_to_load=days, timeframe="1D")
            return True
        except:
            return False

    success_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_update_worker, sym): sym for sym in universe_list}
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                success_count += 1
                
    print(f"✅ Pipeline: Đã cập nhật xong {success_count}/{len(universe_list)} mã.")
    return True