from datetime import datetime
from vnstock.core.utils.logger import get_logger
logger=get_logger(__name__)
def validate_date(date_str:str):
	A=date_str
	try:datetime.strptime(A,'%Y-%m-%d');return True
	except ValueError:logger.error(f"Invalid date format: {A}. Please use the format YYYY-mm-dd.");return False