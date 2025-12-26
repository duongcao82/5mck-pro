import logging
def setup_logger(name:str,level:int=logging.DEBUG)->logging.Logger:
	A=logging.getLogger(name)
	if not A.handlers:B=logging.StreamHandler();C=logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s');B.setFormatter(C);A.addHandler(B);A.setLevel(level)
	return A