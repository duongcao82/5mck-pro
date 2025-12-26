from enum import Enum
from typing import Dict,List,Optional,Any
import pyarrow as pa
class AssetClass(Enum):STOCK='stock';CRYPTO='crypto';FOREX='forex';FUTURES='futures';OPTIONS='options';INDICES='indices';ETF='etf';BOND='bond'
class DataFrequency(Enum):TICK='tick';SECOND='1s';MINUTE='1m';HOUR='1h';DAILY='1d';WEEKLY='1w';MONTHLY='1M';QUARTERLY='1q';YEARLY='1y'
class DataCategory(Enum):MARKET_DATA='market_data';ORDER_BOOK='order_book';TRADES='trades';OHLCV='ohlcv';FUNDAMENTAL='fundamental';SENTIMENT='sentiment';DERIVATIVES='derivatives'
def get_dynamic_schema(df_sample:'pd.DataFrame')->pa.Schema:
	B={'int64':pa.int64(),'float64':pa.float64(),'bool':pa.bool_(),'datetime64[ns]':pa.timestamp('ns'),'object':pa.string()};A=[]
	for(C,D)in df_sample.dtypes.items():E=B.get(str(D),pa.string());A.append(pa.field(C,E))
	return pa.schema(A)