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

    # C. Import Quote
    from vnstock_data import Quote
    
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

def run_bulk_update(tickers_list, days_back=365):
    """
    Chạy cập nhật dữ liệu.
    [FIX] Thêm lại tham số days_back để tương thích với app.py cũ.
    Tuy nhiên logic bên trong vẫn tuân thủ yêu cầu:
    1. D1: 365 ngày (hoặc theo days_back nếu muốn)
    2. 1H: 50 ngày
    3. 15m: 20 ngày
    """
    if not HAS_PIPELINE:
        return "⚠️ Lỗi: Chưa cài đặt thư viện 'vnstock_data'."
        
    try:
        fetcher = AppCacheFetcher()
        exporter = ParquetCacheExporter()
        scheduler = Scheduler(fetcher=fetcher, exporter=exporter, max_workers=10)
        
        end_date = now_vn().strftime('%Y-%m-%d')        
        # --- [BATCH 1] DỮ LIỆU NGÀY (D1) ---
        # Sử dụng tham số days_back để tránh lỗi gọi hàm, mặc định là 365
        start_d1 = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        print(f"🔄 [1/3] Bắt đầu tải D1 ({days_back} ngày)...")
        scheduler.run(
            tickers=tickers_list,
            fetcher_kwargs={'start': start_d1, 'end': end_date, 'interval': '1D'},
            exporter_kwargs={'output_dir': CACHE_DIR, 'interval': '1D'}
        )

        # --- [BATCH 2] DỮ LIỆU 1 GIỜ (1H) - 300 Ngày (Cố định) ---
        start_1h = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')
        print("🔄 [2/3] Bắt đầu tải 1H (300 ngày)...")
        scheduler.run(
            tickers=tickers_list,
            fetcher_kwargs={'start': start_1h, 'end': end_date, 'interval': '1H'},
            exporter_kwargs={'output_dir': CACHE_DIR, 'interval': '1H'}
        )

        # --- [BATCH 3] DỮ LIỆU 15 PHÚT (15m) - 20 Ngày (Cố định) ---
        start_15m = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')
        print("🔄 [3/3] Bắt đầu tải 15m (300 ngày)...")
        scheduler.run(
            tickers=tickers_list,
            fetcher_kwargs={'start': start_15m, 'end': end_date, 'interval': '15m'},
            exporter_kwargs={'output_dir': CACHE_DIR, 'interval': '15m'}
        )
        
        return "✅ Đã cập nhật xong dữ liệu (D1, 1H, 15m)."
    except Exception as e:
        return f"❌ Lỗi Runtime: {str(e)}"

def run_universe_pipeline(tickers_list, days=20):
    """
    Chạy cập nhật SIÊU TỐC cho Universe.
    - Chỉ tải D1.
    - Chỉ tải 20 ngày.
    - Max Workers cao (20) để quét nhanh.
    """
    if not HAS_PIPELINE: return False
    
    try:
        fetcher = AppCacheFetcher()
        exporter = ParquetCacheExporter()
        
        # Tăng Worker lên 20 vì request 1D/20 ngày rất nhẹ, không sợ nghẽn
        scheduler = Scheduler(fetcher=fetcher, exporter=exporter, max_workers=20)
        
        end_date = now_vn().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        print(f"[Pipeline] ⚡ Kích hoạt Universe Bulk Update: {len(tickers_list)} mã (D1, {days} ngày)...")
        
        scheduler.run(
            tickers=tickers_list,
            fetcher_kwargs={'start': start_date, 'end': end_date, 'interval': '1D'},
            exporter_kwargs={'output_dir': CACHE_DIR, 'interval': '1D'}
        )
        return True
    except Exception as e:
        print(f"Pipeline Error: {e}")
        return False