import time,logging
logger=logging.getLogger(__name__)
def retry(func,max_attempts:int=3,backoff:float=2.,*C,**D):
	B=max_attempts;A=0
	while A<B:
		try:return func(*C,**D)
		except Exception as E:A+=1;logger.warning(f"Thử lại {A}/{B} sau lỗi: {E}");time.sleep(backoff**A)
	raise Exception('Hết số lần thử, không thể hoàn thành thao tác.')