_D='25/07/2023'
_C='SSI'
_B='31/07/2023'
_A='data'
from pandas import json_normalize
import pandas as pd,json,time
from typing import Union
from ssi_fc_data import fc_md_client,model
from ssi_fc_data.fc_md_client import MarketDataClient
class StockData:
	def __init__(A,client,config):A.client=client;A.config=config
	def listing(A,market='HOSE',page=1,size=1000)->pd.DataFrame:C=model.securities(market,page,size);B=A.client.securities(A.config,C);print(f"Tổng số bản ghi: {B["totalRecord"]}");return pd.DataFrame(B[_A])
	def details(B,symbol='ACB',market='HOSE',page=1,size=100)->pd.DataFrame:D='ReportDate';E=model.securities_details(market,symbol,page,size);C=B.client.securities_details(B.config,E)[_A];A=json_normalize(C[0]['RepeatedInfo']);A[D]=C[0][D];A=A.dropna(axis=1,how='all');return A
	def daily_ohlc(B,symbol=_C,start='01/07/2023',end=_B,ascending=True,page=1,size=1000)->pd.DataFrame:C=model.daily_ohlc(symbol,start,end,page,size,ascending);D=B.client.daily_ohlc(B.config,C)[_A];A=json_normalize(D);A=A.drop(columns=['Time']);return A
	def intraday_ohlc(A,symbol=_C,start=_D,end=_B,page=1,size=1000,ascending=True,resolution=1)->pd.DataFrame:B=model.intraday_ohlc(symbol,start,end,page,size,ascending,resolution);C=A.client.intraday_ohlc(A.config,B);return json_normalize(C[_A])
	def daily_price(A,symbol=_C,start=_D,end=_B,page=1,size=1000,market='')->pd.DataFrame:B=model.daily_stock_price(symbol,start,end,page,size,market);C=A.client.daily_stock_price(A.config,B)[_A];return json_normalize(C)
class IndexData:
	def __init__(A,client,config):A.client=client;A.config=config
	def listing(A,exchange='',page=1,size=100)->pd.DataFrame:B=model.index_list(exchange,page,size);C=A.client.index_list(A.config,B)[_A];return json_normalize(C)
	def component(C,index='VN30',page=1,size=100)->pd.DataFrame:D=model.index_components(index,page,size);A=C.client.index_components(C.config,D)[_A][0];E=A['IndexCode'];F=A['Exchange'];G=A['TotalSymbolNo'];print(f"Chỉ số: {E} - {F}. Tổng số {G} mã chứng khoán");B=json_normalize(A['IndexComponent']);B=B.drop(columns=['Isin']);return B
	def daily_ohlc(B,index='VN30',start=_D,end=_B,page=1,size=1000,orderBy='Tradingdate',order='desc',request_id='')->pd.DataFrame:C=model.daily_index(request_id,index,start,end,page,size,orderBy,order);D=B.client.daily_index(B.config,C)[_A];A=json_normalize(D);A=A.dropna(axis=1,how='all');return A
class Config:
	def __init__(A,consumer_id:str,consumer_secret:str,access_token:Union[str,None]=None):B='https://fc-data.ssi.com.vn/';A.consumerID=consumer_id;A.consumerSecret=consumer_secret;A.auth_type='Bearer';A.url=B;A.stream_url=B;A.access_jwt=access_token
def get_token(config):A=config;B=fc_md_client.MarketDataClient(A);C=B.access_token(model.accessToken(A.consumerID,A.consumerSecret));return C
class FCData:
	def __init__(A,config):B=config;A.config=B;A.client=fc_md_client.MarketDataClient(B);A.stock=StockData(A.client,B);A.index=IndexData(A.client,B)