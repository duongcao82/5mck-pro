import pandas as pd
from pta_reload import ta
from typing import Union
from.docs import*
class VolumeIndicator:
	def __init__(self,data:pd.DataFrame):"\n        Calculate Volume Indicators.\n\n        Args:\n            data (pd.DataFrame): DataFrame containing price data with columns like 'close'.\n        ";self.data=data
	def obv(self)->pd.Series:obv_series=ta.obv(self.data['close'],self.data['volume'],talib=False);return obv_series
	obv.__doc__=OBV_DOC