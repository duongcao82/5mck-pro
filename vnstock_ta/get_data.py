from vnstock import Vnstock
class DataSource:
	def __init__(A,symbol='VCI',start='2024-01-02',end='2024-06-10',interval='1D',source='VCI'):C=source;B=symbol;A.symbol=B;A.start=start;A.end=end;A.interval=interval;A.source=C;A.quote=Vnstock().stock(symbol=B,source=C).quote;A.data=A.get_data()
	def get_data(A):B=A.quote.history(start=A.start,end=A.end,interval=A.interval);B=B.set_index('time');return B