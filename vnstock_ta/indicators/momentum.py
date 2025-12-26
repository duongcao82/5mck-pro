_B=False
_A='close'
import pandas as pd
from pta_reload import ta
from typing import Union
from.docs import*
class MomentumIndicator:
	def __init__(self,data:pd.DataFrame):"\n        Calculate Momentum Indicators.\n\n        Args:\n            data (pd.DataFrame): DataFrame containing price data with columns like 'close'.\n        ";self.data=data
	def rsi(self,length:int=14)->pd.Series:rsi_data=ta.rsi(self.data[_A],length=length,talib=_B);return rsi_data
	rsi.__doc__=RSI_DOC
	def macd(self,fast:int=12,slow:int=26,signal:int=9)->pd.DataFrame:macd_df=ta.macd(self.data[_A],fast=fast,slow=slow,signal=signal,talib=_B);return macd_df
	macd.__doc__=MACD_DOC
	def willr(self,length:int=14)->pd.Series:willr_data=ta.willr(self.data['high'],self.data['low'],self.data[_A],length=length,talib=_B);return willr_data
	willr.__doc__=WILLR_DOC
	def cmo(self,length:int=9)->pd.Series:cmo_data=ta.cmo(self.data[_A],length=length,talib=_B);return cmo_data
	cmo.__doc__=CMO_DOC
	def stoch(self,k:int=14,d:int=3,smooth_k:int=3)->pd.DataFrame:stoch_data=ta.stoch(self.data['high'],self.data['low'],self.data[_A],k=k,d=d,smooth_k=smooth_k);return stoch_data
	stoch.__doc__=STOCH_DOC
	def roc(self,length:int=9)->pd.Series:roc_data=ta.roc(self.data[_A],length=length,talib=_B);return roc_data
	roc.__doc__=ROC_DOC
	def mom(self,length:int=10)->pd.Series:mom_data=ta.mom(self.data[_A],length=length,talib=_B);return mom_data
	mom.__doc__=MOM_DOC