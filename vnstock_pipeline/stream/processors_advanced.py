_I='Redis connection closed'
_H='redis module not installed. Install with: pip install redis'
_G='redis://localhost:6379'
_F='close'
_E=True
_D='{data_type}'
_C='unknown'
_B='data_type'
_A=None
import asyncio,json,traceback
from typing import Dict,Any,Optional,Callable,List
from collections import deque
import time
from vnstock_pipeline.stream.processors import DataProcessor
class RedisStreamProcessor(DataProcessor):
	def __init__(A,redis_url:str=_G,stream_key:str='market:stream',maxlen:int=10000,data_types:Optional[List[str]]=_A):
		B=data_types;super().__init__();A.redis_url=redis_url;A.stream_key=stream_key;A.maxlen=maxlen;A.data_types=set(B)if B else _A;A.redis=_A
		try:import redis.asyncio as C;A.aioredis=C
		except ImportError:A.logger.error(_H);raise
	async def _ensure_connected(A):
		if A.redis is _A:A.redis=await A.aioredis.from_url(A.redis_url,encoding='utf-8',decode_responses=_E);A.logger.info(f"Connected to Redis: {A.redis_url}")
	def _get_stream_key(B,data:Dict[str,Any])->str:
		A=B.stream_key
		if _D in A:C=data.get(_B,_C);A=A.replace(_D,C)
		return A
	async def process(A,data:Dict[str,Any])->_A:
		B=data
		try:
			if A.data_types:
				D=B.get(_B,_C)
				if D not in A.data_types:return
			await A._ensure_connected();C=A._get_stream_key(B);await A.redis.xadd(C,{'data':json.dumps(B)},maxlen=A.maxlen);A.logger.debug(f"Streamed to Redis: {C}")
		except Exception as E:A.logger.error(f"Redis streaming error: {E}");A.logger.error(traceback.format_exc())
	async def close(A):
		if A.redis:await A.redis.close();A.logger.info(_I)
class RedisPubSubProcessor(DataProcessor):
	def __init__(A,redis_url:str=_G,channel:str='market:data',data_types:Optional[List[str]]=_A):
		B=data_types;super().__init__();A.redis_url=redis_url;A.channel=channel;A.data_types=set(B)if B else _A;A.redis=_A
		try:import redis.asyncio as C;A.aioredis=C
		except ImportError:A.logger.error(_H);raise
	async def _ensure_connected(A):
		if A.redis is _A:A.redis=await A.aioredis.from_url(A.redis_url,encoding='utf-8',decode_responses=_E);A.logger.info(f"Connected to Redis: {A.redis_url}")
	def _get_channel(B,data:Dict[str,Any])->str:
		A=B.channel
		if _D in A:C=data.get(_B,_C);A=A.replace(_D,C)
		return A
	async def process(A,data:Dict[str,Any])->_A:
		B=data
		try:
			if A.data_types:
				D=B.get(_B,_C)
				if D not in A.data_types:return
			await A._ensure_connected();C=A._get_channel(B);await A.redis.publish(C,json.dumps(B));A.logger.debug(f"Published to channel: {C}")
		except Exception as E:A.logger.error(f"Redis pub/sub error: {E}");A.logger.error(traceback.format_exc())
	async def close(A):
		if A.redis:await A.redis.close();A.logger.info(_I)
class KafkaProcessor(DataProcessor):
	def __init__(A,bootstrap_servers:str='localhost:9092',topic:str='market.data',key_field:Optional[str]=_A,data_types:Optional[List[str]]=_A,compression_type:str='gzip'):
		B=data_types;super().__init__();A.bootstrap_servers=bootstrap_servers;A.topic=topic;A.key_field=key_field;A.data_types=set(B)if B else _A;A.compression_type=compression_type;A.producer=_A
		try:from aiokafka import AIOKafkaProducer as C;A.AIOKafkaProducer=C
		except ImportError:A.logger.error('aiokafka module not installed. Install with: pip install aiokafka');raise
	async def _ensure_connected(A):
		if A.producer is _A:A.producer=A.AIOKafkaProducer(bootstrap_servers=A.bootstrap_servers,value_serializer=lambda v:json.dumps(v).encode(),key_serializer=lambda k:k.encode()if k else _A,compression_type=A.compression_type);await A.producer.start();A.logger.info(f"Kafka producer started: {A.bootstrap_servers}")
	def _get_topic(B,data:Dict[str,Any])->str:
		A=B.topic
		if _D in A:C=data.get(_B,_C);A=A.replace(_D,C)
		return A
	def _get_key(A,data:Dict[str,Any])->Optional[str]:
		if A.key_field:return str(data.get(A.key_field,''))
	async def process(A,data:Dict[str,Any])->_A:
		B=data
		try:
			if A.data_types:
				E=B.get(_B,_C)
				if E not in A.data_types:return
			await A._ensure_connected();C=A._get_topic(B);D=A._get_key(B);await A.producer.send(C,value=B,key=D);A.logger.debug(f"Sent to Kafka: {C} (key={D})")
		except Exception as F:A.logger.error(f"Kafka sending error: {F}");A.logger.error(traceback.format_exc())
	async def close(A):
		if A.producer:await A.producer.stop();A.logger.info('Kafka producer stopped')
class WebSocketRelayProcessor(DataProcessor):
	def __init__(A,host:str='0.0.0.0',port:int=8765,data_types:Optional[List[str]]=_A,max_clients:int=100):
		B=data_types;super().__init__();A.host=host;A.port=port;A.data_types=set(B)if B else _A;A.max_clients=max_clients;A.clients=set();A.server=_A;A.server_task=_A
		try:import websockets as C;A.websockets=C
		except ImportError:A.logger.error('websockets module not installed. Install with: pip install websockets');raise
	async def _start_server(A):
		if A.server is _A:A.server=await A.websockets.serve(A._handle_client,A.host,A.port);A.logger.info(f"WebSocket relay started: ws://{A.host}:{A.port}")
	async def _handle_client(A,websocket,path):
		B=websocket
		if len(A.clients)>=A.max_clients:await B.close(1008,'Max clients reached');A.logger.warning('Rejected client: max clients reached');return
		A.clients.add(B);A.logger.info(f"Client connected: {B.remote_address} (total: {len(A.clients)})")
		try:
			async for C in B:await B.send(C)
		except Exception as D:A.logger.debug(f"Client connection error: {D}")
		finally:A.clients.remove(B);A.logger.info(f"Client disconnected (total: {len(A.clients)})")
	async def process(A,data:Dict[str,Any])->_A:
		try:
			if A.data_types:
				E=data.get(_B,_C)
				if E not in A.data_types:return
			await A._start_server()
			if not A.clients:return
			F=json.dumps(data);C=set()
			for D in A.clients:
				try:await D.send(F)
				except Exception as B:A.logger.debug(f"Failed to send to client: {B}");C.add(D)
			A.clients-=C
			if A.clients:A.logger.debug(f"Broadcasted to {len(A.clients)} clients")
		except Exception as B:A.logger.error(f"WebSocket relay error: {B}");A.logger.error(traceback.format_exc())
	async def close(A):
		if A.server:A.server.close();await A.server.wait_closed();A.logger.info('WebSocket relay stopped')
class BufferedProcessor(DataProcessor):
	def __init__(A,processor:DataProcessor,batch_size:int=100,flush_interval:float=5.):super().__init__();A.processor=processor;A.batch_size=batch_size;A.flush_interval=flush_interval;A.buffer=deque();A.last_flush=time.time();A.flush_task=_A
	async def _auto_flush(A):
		while _E:
			await asyncio.sleep(A.flush_interval)
			if A.buffer and time.time()-A.last_flush>=A.flush_interval:await A._flush()
	async def _flush(A):
		if not A.buffer:return
		B=list(A.buffer);A.buffer.clear();A.last_flush=time.time();A.logger.debug(f"Flushing {len(B)} items")
		for C in B:
			try:await A.processor.process(C)
			except Exception as D:A.logger.error(f"Error processing buffered data: {D}")
	async def process(A,data:Dict[str,Any])->_A:
		try:
			if A.flush_task is _A:A.flush_task=asyncio.create_task(A._auto_flush())
			A.buffer.append(data)
			if len(A.buffer)>=A.batch_size:await A._flush()
		except Exception as B:A.logger.error(f"Buffered processor error: {B}");A.logger.error(traceback.format_exc())
	async def close(A):
		if A.flush_task:A.flush_task.cancel()
		await A._flush()
		if hasattr(A.processor,_F):
			if asyncio.iscoroutinefunction(A.processor.close):await A.processor.close()
			else:A.processor.close()
class ConditionalProcessor(DataProcessor):
	def __init__(A,processor:DataProcessor,condition:Callable[[Dict[str,Any]],bool]):super().__init__();A.processor=processor;A.condition=condition
	async def process(A,data:Dict[str,Any])->_A:
		try:
			if A.condition(data):await A.processor.process(data)
			else:A.logger.debug('Condition not met, skipping')
		except Exception as B:A.logger.error(f"Conditional processor error: {B}");A.logger.error(traceback.format_exc())
	async def close(A):
		if hasattr(A.processor,_F):
			if asyncio.iscoroutinefunction(A.processor.close):await A.processor.close()
			else:A.processor.close()
class MultiProcessor(DataProcessor):
	def __init__(A,processors:List[DataProcessor]):B=processors;super().__init__();A.processors=B;A.logger.info(f"MultiProcessor with {len(B)} processors")
	async def process(A,data:Dict[str,Any])->_A:
		try:
			C=[A.process(data)for A in A.processors];D=await asyncio.gather(*C,return_exceptions=_E)
			for(E,B)in enumerate(D):
				if isinstance(B,Exception):A.logger.error(f"Processor {A.processors[E].__class__.__name__} error: {B}")
		except Exception as F:A.logger.error(f"Multi-processor error: {F}");A.logger.error(traceback.format_exc())
	async def close(B):
		for A in B.processors:
			if hasattr(A,_F):
				try:
					if asyncio.iscoroutinefunction(A.close):await A.close()
					else:A.close()
				except Exception as C:B.logger.error(f"Error closing processor: {C}")