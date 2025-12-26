import os,sys,json
from vnii import*
from vnstock_ta.utils.const import PROJECT_DIR,ID_DIR
class SystemInfo:
	def __init__(self):0
	def _is_jpylab(self):
		A=False
		try:
			shell=get_ipython().__class__.__name__
			if shell=='ZMQInteractiveShell':
				if'JPY_PARENT_PID'in os.environ or'JPY_USER'in os.environ:return True
			return A
		except NameError:return A
	def interface(self):
		B='Other';A='Terminal'
		try:
			from IPython import get_ipython
			if'IPKernelApp'not in get_ipython().config:
				if sys.stdout.isatty():return A
				else:return B
			else:return'Jupyter'
		except(ImportError,AttributeError):
			if sys.stdout.isatty():return A
			else:return B
	def hosting(self):
		B='Local or Unknown';A='SPACE_HOST'
		try:
			if'google.colab'in sys.modules:return'Google Colab'
			if self._is_jpylab():return'JupyterLab'
			elif'CODESPACE_NAME'in os.environ:return'Github Codespace'
			elif'GITPOD_WORKSPACE_CLUSTER_HOST'in os.environ:return'Gitpod'
			elif'REPLIT_USER'in os.environ:return'Replit'
			elif'KAGGLE_CONTAINER_NAME'in os.environ:return'Kaggle'
			elif A in os.environ and'.hf.space'in os.environ[A]:return'Hugging Face Spaces'
			else:return B
		except KeyError:return B
	def os(self):
		try:
			platform=sys.platform
			if platform.startswith('linux'):return'Linux'
			elif platform=='darwin':return'macOS'
			elif platform=='win32':return'Windows'
			else:return'Unknown'
		except Exception as e:return f"Error determining OS: {str(e)}"
lc_init()
def idv():
	A='Không tìm thấy thông tin người dùng hợp lệ. Vui lòng liên hệ Vnstock để được hỗ trợ!';id=PROJECT_DIR/'user.json'
	if not os.path.exists(id):raise SystemExit(A)
	else:
		with open(id,'r')as f:id=json.load(f)
		if not id['user']:raise SystemExit(A)
		return'Valid user!'