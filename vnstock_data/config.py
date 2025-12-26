import logging
class Config:
	REQUEST_TIMEOUT:int=30;RETRIES:int=3;BACKOFF_MULTIPLIER:float=1.;BACKOFF_MIN:float=2;BACKOFF_MAX:float=10;CACHE_SIZE:int=128;LOG_LEVEL:int=logging.DEBUG
	@classmethod
	def apply_logging_config(A):logging.getLogger('vnstock').setLevel(A.LOG_LEVEL)