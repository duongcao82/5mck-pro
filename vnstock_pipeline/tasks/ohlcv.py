_B='2025-03-19'
_A='2024-01-01'
import pandas as pd
from vnstock_pipeline.template.vnstock import VNFetcher,VNValidator,VNTransformer
from vnstock_pipeline.utils.deduplication import drop_duplicates
from vnstock_data.explorer.vci import Quote
class OHLCVDailyFetcher(VNFetcher):
	def _vn_call(G,ticker:str,**A)->pd.DataFrame:B=A.get('start',_A);C=A.get('end',_B);D=A.get('interval','1D');E=Quote(symbol=ticker);F=E.history(start=B,end=C,interval=D);return F
class OHLCVDailyValidator(VNValidator):required_columns=['time','open','high','low','close','volume']
class OHLCVDailyTransformer(VNTransformer):
	def transform(B,data:pd.DataFrame)->pd.DataFrame:A=super().transform(data);A=drop_duplicates(A,subset=['time']);return A
def run_task(tickers:list,**B):
	from vnstock_pipeline.core.scheduler import Scheduler as C;from vnstock_pipeline.core.exporter import CSVExport as D;A=OHLCVDailyFetcher();E=OHLCVDailyValidator();F=OHLCVDailyTransformer();G=D(base_path='./data/ohlcv');A.params=B;H=A.fetch
	def I(ticker:str):return H(ticker,**A.params or{})
	A.fetch=I;J=C(A,E,F,G);J.run(tickers)
if __name__=='__main__':sample_tickers=['ACB','VCB','HPG'];run_task(sample_tickers,start=_A,end=_B,interval='1D')