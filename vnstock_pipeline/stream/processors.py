_K='callback'
_J='socket'
_I='market_data'
_H='event_type'
_G='webhook'
_F=', '
_E=False
_D='data_type'
_C='unknown'
_B=True
_A=None
import csv,json,logging,os,time,traceback,datetime
from abc import ABC,abstractmethod
from typing import Dict,Any,List,Tuple,Set,Optional
from vnstock_pipeline.stream.utils.column_mappings import get_column_order
class DataProcessor(ABC):
	def __init__(A):A.logger=logging.getLogger(A.__class__.__name__)
	@abstractmethod
	async def process(self,data:Dict[str,Any])->_A:0
class ConsoleProcessor(DataProcessor):
	def __init__(A,pretty_print:bool=_B):super().__init__();A.pretty_print=pretty_print
	async def process(A,data:Dict[str,Any])->_A:
		try:
			if A.pretty_print:print(f"Received: {json.dumps(data,indent=2)}")
			else:print(f"Received: {data}")
		except Exception as B:A.logger.error(f"Error in ConsoleProcessor: {B}");A.logger.error(traceback.format_exc())
class CSVProcessor(DataProcessor):
	def __init__(A,filename_template:str='market_data_{event_type}_%Y-%m-%d.csv'):super().__init__();A.filename_template=filename_template;A.files={};A.fields={}
	def _get_filename(B,data:Dict[str,Any])->str:C=data.get(_H,_C);A=B.filename_template.replace('{event_type}',C);A=datetime.datetime.now().strftime(A);return A
	def _create_new_file(B,filename:str,fields:List[str],event_type:str=_A):
		H=fields;D=event_type;A=filename
		try:
			if D:E=get_column_order(D)
			else:E=[]
			C=[]
			if E:
				for I in E:
					if I in H:C.append(I)
			K=sorted(set(H)-set(C));C.extend(K);os.makedirs(os.path.dirname(A)or'.',exist_ok=_B);J=os.path.exists(A);L='a'if J else'w';F=open(A,L,newline='');G=csv.DictWriter(F,fieldnames=C)
			if not J:G.writeheader();B.logger.info(f"Created new CSV {A}: {len(C)} fields (event_type={D})")
			else:B.logger.info(f"Appending to existing CSV {A}")
			B.files[A]=F,G;B.fields[A]=set(C);return F,G
		except Exception as M:B.logger.error(f"Error creating CSV {A}: {M}");B.logger.error(traceback.format_exc());raise
	async def process(A,data:Dict[str,Any])->_A:
		D=data
		try:
			B=A._get_filename(D);G=D.get(_H,_C);J={B:A for(B,A)in D.items()if A is not _A and A!=''};F=set(J.keys())
			if B in A.files:
				C,E=A.files[B];H=A.fields[B];I=F-H
				if I:K=list(H.union(F));A.logger.info(f"Adding {len(I)} new fields to {B}");C.close();C,E=A._create_new_file(B,K,G);A.files[B]=C,E
			else:C,E=A._create_new_file(B,list(F),G)
			L={A:D.get(A,_A)for A in A.fields[B]};E.writerow(L);C.flush()
		except Exception as M:A.logger.error(f"Error writing to CSV: {M}");A.logger.error(traceback.format_exc())
	def close(A):
		for(B,(C,E))in A.files.items():
			try:C.close();A.logger.debug(f"Closed file: {B}")
			except Exception as D:A.logger.error(f"Error closing {B}: {D}")
class DuckDBProcessor(DataProcessor):
	def __init__(A,db_path:str,table_prefix:str=_I):
		B=db_path;super().__init__()
		try:import duckdb as C;os.makedirs(os.path.dirname(B)or'.',exist_ok=_B);A.con=C.connect(B);A.table_prefix=table_prefix;A.tables={};A.logger.info(f"DuckDB initialized: {B}")
		except ImportError:A.logger.error("DuckDB module not installed. Install with 'pip install duckdb'");raise
		except Exception as D:A.logger.error(f"DuckDB initialization error: {D}");A.logger.error(traceback.format_exc());raise
	def _get_table_name(A,data:Dict[str,Any])->str:B=data.get(_D,_C);return f"{A.table_prefix}_{B}"
	def _ensure_table_exists(A,table_name:str,data:Dict[str,Any])->_A:
		I=data;B=table_name
		try:
			if B in A.tables:
				K=A.tables[B]
				for(C,D)in I.items():
					if C not in K:
						E=A._infer_type(D);F=f'ALTER TABLE "{B}" ADD COLUMN "{C}" {E}'
						try:A.con.execute(F);A.tables[B].add(C);A.logger.info(f"Added column {C} to {B}")
						except Exception as G:A.logger.error(f"Failed to add column {C}: {G}");A.logger.error(traceback.format_exc())
			else:
				try:
					A.con.execute(f'SELECT * FROM "{B}" LIMIT 0');L=A.con.execute(f'PRAGMA table_info("{B}")');H={A[1]for A in L.fetchall()};A.tables[B]=H
					for(C,D)in I.items():
						if C not in H:
							E=A._infer_type(D);F=f'ALTER TABLE "{B}" ADD COLUMN "{C}" {E}'
							try:A.con.execute(F);A.tables[B].add(C);A.logger.info(f"Added column {C} to {B}")
							except Exception as G:A.logger.error(f"Failed to add column {C}: {G}");A.logger.error(traceback.format_exc())
				except Exception:
					H=[];J=set()
					for(C,D)in I.items():E=A._infer_type(D);H.append(f'"{C}" {E}');J.add(C)
					F=f'CREATE TABLE "{B}" ({_F.join(H)})';A.con.execute(F);A.tables[B]=J;A.logger.info(f"Created table {B}")
		except Exception as G:A.logger.error(f"Error ensuring table exists: {G}");A.logger.error(traceback.format_exc());raise
	def _infer_type(C,value:Any)->str:
		B='VARCHAR';A=value
		if A is _A:return B
		elif isinstance(A,bool):return'BOOLEAN'
		elif isinstance(A,int):return'BIGINT'
		elif isinstance(A,float):return'DOUBLE'
		elif isinstance(A,str):return B
		elif isinstance(A,(list,dict)):return'JSON'
		elif isinstance(A,datetime.datetime):return'TIMESTAMP'
		elif isinstance(A,datetime.date):return'DATE'
		else:return B
	def _sanitize_value(B,value:Any)->Any:
		A=value
		if isinstance(A,int)and(A>2147483647 or A<-2147483648):return str(A)
		return A
	async def process(A,data:Dict[str,Any])->_A:
		C=data
		try:
			B=A._get_table_name(C);A._ensure_table_exists(B,C)
			if B in A.tables:D={C:D for(C,D)in C.items()if C in A.tables[B]}
			else:D=C
			if not D:A.logger.warning(f"No valid columns to insert into {B}");return
			E={B:A._sanitize_value(C)for(B,C)in D.items()};F=_F.join([f'"{A}"'for A in E.keys()]);G=_F.join(['?'for A in E.keys()]);H=list(E.values());I=f'INSERT INTO "{B}" ({F}) VALUES ({G})';A.con.execute(I,H);A.logger.debug(f"Inserted data into {B}")
		except Exception as J:A.logger.error(f"Error inserting data into DuckDB: {J}");A.logger.error(traceback.format_exc())
	def close(A):
		try:A.con.close();A.logger.info('DuckDB connection closed')
		except Exception as B:A.logger.error(f"Error closing DuckDB connection: {B}")
class FilteredProcessor(DataProcessor):
	def __init__(A,wrapped_processor:DataProcessor,allowed_data_types:List[str]):B=allowed_data_types;super().__init__();A.wrapped_processor=wrapped_processor;A.allowed_data_types=set(B);A.logger.info(f"Created filtered processor for data types: {_F.join(B)}")
	async def process(A,data:Dict[str,Any])->_A:
		B=data.get(_D,_C)
		if B in A.allowed_data_types:await A.wrapped_processor.process(data)
		else:A.logger.debug(f"Filtered out data of type: {B}")
	def close(A):
		if hasattr(A.wrapped_processor,'close'):A.wrapped_processor.close()
class FirebaseProcessor(DataProcessor):
	def __init__(A,collection_prefix:str=_I,service_account_path:str=_A):
		C=service_account_path;B=collection_prefix;super().__init__()
		try:
			import firebase_admin as D;from firebase_admin import credentials as E,firestore as G
			if not D._apps:
				if C:F=E.Certificate(C)
				else:F=E.ApplicationDefault()
				D.initialize_app(F)
			A.db=G.client();A.collection_prefix=B;A.logger.info(f"Firebase Firestore initialized with prefix: {B}")
		except ImportError:A.logger.error("Firebase modules not installed. Install with 'pip install firebase-admin'");raise
		except Exception as H:A.logger.error(f"Firebase initialization error: {H}");A.logger.error(traceback.format_exc());raise
	def _get_collection_name(A,data:Dict[str,Any])->str:B=data.get(_D,_C);return f"{A.collection_prefix}_{B}"
	def _get_document_id(E,data:Dict[str,Any])->str:
		D='index_id';C='ticker';A=data;B=A.get('timestamp',time.time())
		if A.get(_D)=='price'and C in A:return f"{A[C]}_{B}"
		elif A.get(_D)=='index'and D in A:return f"{A[D]}_{B}"
		else:return f"{B}"
	async def process(A,data:Dict[str,Any])->_A:
		B=data
		try:C=A._get_collection_name(B);D=A._get_document_id(B);A.db.collection(C).document(D).set(B);A.logger.debug(f"Data saved to Firestore: {C}/{D}")
		except Exception as E:A.logger.error(f"Error saving to Firestore: {E}");A.logger.error(traceback.format_exc())
class ForwardingProcessor(DataProcessor):
	def __init__(A,forward_url:str,forward_type:str=_G,timeout:int=5):
		C=forward_url;B=forward_type;super().__init__();A.forward_url=C;A.forward_type=B.lower();A.timeout=timeout;A.callback=_A;A.session=_A
		if A.forward_type==_G:
			try:import aiohttp as D
			except ImportError:A.logger.warning("aiohttp not installed. Install with 'pip install aiohttp' for webhook support")
		elif A.forward_type==_J:A.socket=_A;A._init_socket()
		elif A.forward_type!=_K:A.logger.warning(f"Unknown forward type: {B}. Supported: webhook, socket, callback")
		A.logger.info(f"ForwardingProcessor initialized: {B} -> {C}")
	def _init_socket(A):
		try:import socket as B;A.socket_module=B;A.socket=_A
		except Exception as C:A.logger.error(f"Socket initialization error: {C}")
	def set_callback(A,callback):A.callback=callback;A.logger.info('Custom callback set for data forwarding')
	async def _forward_webhook(A,data:Dict[str,Any])->bool:
		try:
			import aiohttp as C
			async with C.ClientSession()as D:
				async with D.post(A.forward_url,json=data,timeout=C.ClientTimeout(total=A.timeout))as B:
					if B.status==200:A.logger.debug(f"Webhook forward successful: {B.status}");return _B
					else:A.logger.warning(f"Webhook returned {B.status}: {await B.text()}");return _E
		except Exception as E:A.logger.error(f"Webhook forward error: {E}");return _E
	async def _forward_socket(A,data:Dict[str,Any])->bool:
		try:import json,asyncio as D;E,B=A.forward_url.split(':');B=int(B);H,C=await D.wait_for(D.open_connection(E,B),timeout=A.timeout);F=(json.dumps(data)+'\n').encode();C.write(F);await C.drain();C.close();A.logger.debug(f"Socket forward successful: {E}:{B}");return _B
		except Exception as G:A.logger.error(f"Socket forward error: {G}");return _E
	async def _forward_callback(A,data:Dict[str,Any])->bool:
		try:
			if A.callback is _A:A.logger.warning('No callback set');return _E
			B=await A.callback(data);A.logger.debug(f"Callback forward result: {B}");return bool(B)
		except Exception as C:A.logger.error(f"Callback forward error: {C}");return _E
	async def process(A,data:Dict[str,Any])->_A:
		B=data
		try:
			if A.forward_type==_G:await A._forward_webhook(B)
			elif A.forward_type==_J:await A._forward_socket(B)
			elif A.forward_type==_K:await A._forward_callback(B)
			else:A.logger.warning(f"Unknown forward type: {A.forward_type}")
		except Exception as C:A.logger.error(f"Error in ForwardingProcessor: {C}");A.logger.error(traceback.format_exc())