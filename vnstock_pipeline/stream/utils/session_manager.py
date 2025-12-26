_B=False
_A=None
import asyncio,datetime,logging,pytz
from typing import Optional,Dict,Any,Callable,Awaitable,List
try:from vnstock.core.utils.market import trading_hours
except ImportError:from vnstock_pipeline.stream.utils.market_hours import trading_hours
logger=logging.getLogger(__name__)
class SessionManager:
	def __init__(A,market:str='HOSE',check_interval:int=60,reconnect_interval:int=300,pre_market_window:int=30,post_market_window:int=30):A.market=market;A.check_interval=check_interval;A.reconnect_interval=reconnect_interval;A.pre_market_window=pre_market_window;A.post_market_window=post_market_window;A.monitoring_task=_A;A.connect_handler=_A;A.disconnect_handler=_A;A.connection_health_checker=_A;A.is_connected=_B;A.last_connect_time=_A;A.scheduled_tasks=[];A.reconnect_event=asyncio.Event();A.current_session_type=_A;A.data_status=_A;A.last_data_time=_A
	def register_connect_handler(A,handler:Callable[[],Awaitable[_A]]):A.connect_handler=handler
	def register_disconnect_handler(A,handler:Callable[[],Awaitable[_A]]):A.disconnect_handler=handler
	def register_connection_health_checker(A,checker:Callable[[],bool]):A.connection_health_checker=checker
	def mark_disconnected(A):
		if A.is_connected:logger.info('Connection marked as disconnected');A.is_connected=_B;A.reconnect_event.set()
	async def start_monitoring(A):logger.info(f"Starting market session monitoring for {A.market}");A.reconnect_event.clear();await A._check_and_handle_session();A.monitoring_task=asyncio.create_task(A._monitor_market_sessions());await A.monitoring_task
	async def stop_monitoring(A):
		logger.info('Stopping market session monitoring')
		if A.monitoring_task:
			A.monitoring_task.cancel()
			try:await A.monitoring_task
			except asyncio.CancelledError:pass
			A.monitoring_task=_A
		for B in A.scheduled_tasks:
			if not B.done():
				B.cancel()
				try:await B
				except asyncio.CancelledError:pass
		A.scheduled_tasks=[]
		if A.is_connected and A.disconnect_handler:await A.disconnect_handler();A.is_connected=_B
	async def trigger_reconnect(A):logger.info('Manual reconnection triggered');A.reconnect_event.set()
	async def _monitor_market_sessions(A):
		try:
			while True:
				try:
					await A._check_and_handle_session()
					try:await asyncio.wait_for(A.reconnect_event.wait(),timeout=A.check_interval);A.reconnect_event.clear();await A._perform_reconnection('Manual trigger')
					except asyncio.TimeoutError:pass
				except Exception as B:logger.error(f"Error in market session monitoring: {B}");await asyncio.sleep(A.check_interval)
		except asyncio.CancelledError:logger.info('Market session monitoring cancelled');raise
	async def _check_and_handle_session(A):
		F='continuous';C=trading_hours(A.market);G=C['is_trading_hour'];B=C['trading_session'];D=C['data_status'];K=C['time']
		if A.connection_health_checker:
			H=A.connection_health_checker()
			if A.is_connected and not H:logger.warning('Connection health check failed - marking as disconnected');A.is_connected=_B
		if B!=A.current_session_type or D!=A.data_status:logger.info(f"Market session changed: {B} (Data: {D})");A.current_session_type=B;A.data_status=D
		I=pytz.timezone('Asia/Ho_Chi_Minh');J=datetime.datetime.now(I);E=J.strftime('%H:%M')
		if E=='12:59'and B=='lunch_break':
			logger.info('Critical time detected: 12:59. Scheduling immediate reconnection for afternoon session.')
			if A.is_connected:await A._perform_reconnection('Scheduled reconnection for afternoon session');await asyncio.sleep(55)
		elif E=='11:28'and B==F:
			logger.info('Critical time detected: 11:28. Scheduling reconnection before lunch break.')
			if A.is_connected:await A._perform_reconnection('Scheduled reconnection before lunch break');await asyncio.sleep(55)
		elif B=='pre_market'and D=='preparing':
			if not A.is_connected:await A._perform_connection('Pre-market preparation')
		elif B in[F,'ato','atc']and G:
			if not A.is_connected:logger.info('Trading session active but not connected - reconnecting');await A._perform_connection('Trading session active')
		elif B in['post_market','after_hours','weekend']:
			if A.is_connected:await A._perform_disconnection('Market closed')
	async def _perform_connection(A,reason:str):
		if not A.connect_handler:logger.warning('No connect handler registered');return
		logger.info(f"Connecting to market data feed: {reason}")
		try:await A.connect_handler();A.is_connected=True;A.last_connect_time=datetime.datetime.now();logger.info('Connection successful')
		except Exception as B:logger.error(f"Connection failed: {B}");A.is_connected=_B
	async def _perform_disconnection(A,reason:str):
		if not A.disconnect_handler:logger.warning('No disconnect handler registered');return
		logger.info(f"Disconnecting from market data feed: {reason}")
		try:await A.disconnect_handler();A.is_connected=_B;logger.info('Disconnection successful')
		except Exception as B:logger.error(f"Disconnection failed: {B}")
	async def _perform_reconnection(A,reason:str):
		B='Reconnection';logger.info(f"Reconnecting to market data feed: {reason}")
		if A.is_connected:await A._perform_disconnection(B)
		await asyncio.sleep(2);await A._perform_connection(B)
	async def _schedule_reconnection(B,delay_seconds:float):
		A=delay_seconds
		try:await asyncio.sleep(A);await B._perform_reconnection(f"Scheduled after {A:.1f}s")
		except asyncio.CancelledError:logger.info('Scheduled reconnection cancelled');raise