# src/pipeline_manager.py
import os
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
        interval = fetcher_kwargs.get('interval', 'Unknown')
        print(f"🚀 [Pipeline] Đang tải {len(tickers)} mã (Khung: {interval})...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_ticker = {
                executor.submit(self._process_task, t, fetcher_kwargs, exporter_kwargs): t 
                for t in tickers
            }
            
            for future in concurrent.futures.as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    success, msg = future.result()
                except Exception as exc:
                    print(f"❌ {ticker} Exception: {exc}")
        return "Xong"

Scheduler = SimpleScheduler

# ==============================================================================
# 3. CẤU HÌNH PIPELINE
# ==============================================================================

# Tìm class AppCacheFetcher trong src/pipeline_manager.py
# Sửa phương thức _vn_call như sau:

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
        
        # --- LOGIC MỚI: Thử TCBS trước tiên ---
        try:
            # Ưu tiên 1: TCBS (Nhanh và ổn định)
            quote = Quote(source='tcbs', symbol=ticker)
            df = quote.history(start=start, end=end, interval=interval)
        except Exception:
            try:
                # Ưu tiên 2: VND (Dữ liệu lịch sử tốt)
                quote = Quote(source='vnd', symbol=ticker)
                df = quote.history(start=start, end=end, interval=interval)
            except Exception:
                try:
                    # Ưu tiên 3: VCI (Dự phòng cuối cùng)
                    quote = Quote(source='vci', symbol=ticker)
                    df = quote.history(start=start, end=end, interval=interval)
                except:
                    return pd.DataFrame()
        
        if df.empty:
            raise ValueError(f"No data for {ticker}")
            
        # ... (Phần chuẩn hóa cột phía dưới giữ nguyên) ...
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
# 4. HÀM CHẠY CHÍNH (ĐÃ FIX LỖI THAM SỐ)
# ==============================================================================

def run_bulk_update(tickers_list, days_back=200):
    """
    Cập nhật dữ liệu đa khung thời gian:
    - D1: 365 ngày
    - 1H: 50 ngày
    - 15m: 20 ngày
    """
    if not HAS_PIPELINE:
        return "⚠️ Lỗi: Chưa cài đặt thư viện 'vnstock_data'."
        
    try:
        fetcher = AppCacheFetcher()
        exporter = ParquetCacheExporter()
        scheduler = Scheduler(fetcher=fetcher, exporter=exporter, max_workers=10)
        
        # 1. Lấy thời gian hiện tại theo VN để đồng nhất
        now = now_vn() 
        end_date = now.strftime('%Y-%m-%d')
        
        # 2. Định nghĩa các cấu hình tải
        configs = [
            {"label": "D1", "days": days_back, "interval": "1D"},
            {"label": "1H", "days": 30, "interval": "1H"},
            {"label": "15m", "days": 12, "interval": "15m"}
        ]
        
        # 3. Chạy vòng lặp cập nhật
        for i, cfg in enumerate(configs, 1):
            start_date = (now - timedelta(days=cfg['days'])).strftime('%Y-%m-%d')
            print(f"🔄 [{i}/3] Đang tải {cfg['label']} ({cfg['days']} ngày) | Từ {start_date} đến {end_date}")
            
            scheduler.run(
                tickers=tickers_list,
                fetcher_kwargs={'start': start_date, 'end': end_date, 'interval': cfg['interval']},
                exporter_kwargs={'output_dir': CACHE_DIR, 'interval': cfg['interval']}
            )
        
        return f"✅ Đã cập nhật xong dữ liệu: D1 ({days_back}d), 1H (50d), 15m (20d)."
        
    except Exception as e:
        return f"❌ Lỗi Runtime: {str(e)}"

# Cập nhật trong pipeline_manager.py
def run_universe_pipeline(universe_list, days=20):
    """
    Tối ưu hóa quy trình cập nhật Universe:
    1. Chỉ lấy D1 (nhẹ) để kiểm tra thanh khoản.
    2. Dùng ThreadPool để chạy nhanh trên 20 workers.
    """
    from data import load_data_with_cache
    import concurrent.futures

    print(f"⚡ Pipeline: Đang quét {len(universe_list)} mã (D1)...")
    
    # Hàm con để update 1 mã
    def _update_worker(sym):
        try:
            # Load 20 ngày gần nhất để tính thanh khoản là đủ
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