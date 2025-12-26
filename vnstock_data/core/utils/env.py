import os,sys,json,importlib.metadata
from..const import PACKAGE_MAPPING
from vnii import lc_init
from vnstock_data.core.utils.const import PROJECT_DIR,ID_DIR
def get_packages_info(package_mapping=PACKAGE_MAPPING):
	A={}
	for(B,D)in package_mapping.items():
		A[B]=[]
		for C in D:
			try:E=importlib.metadata.version(C);A[B].append(C+' '+E)
			except importlib.metadata.PackageNotFoundError:pass
	return A
class SystemInfo:
	def __init__(A):0
	def _is_jpylab(C):
		A=False
		try:
			B=get_ipython().__class__.__name__
			if B=='ZMQInteractiveShell':
				if'JPY_PARENT_PID'in os.environ or'JPY_USER'in os.environ:return True
			return A
		except NameError:return A
	def interface(D):
		B='Other';A='Terminal'
		try:
			from IPython import get_ipython as C
			if'IPKernelApp'not in C().config:
				if sys.stdout.isatty():return A
				else:return B
			else:return'Jupyter'
		except(ImportError,AttributeError):
			if sys.stdout.isatty():return A
			else:return B
	def hosting(C):
		B='Local or Unknown';A='SPACE_HOST'
		try:
			if'google.colab'in sys.modules:return'Google Colab'
			if C._is_jpylab():return'JupyterLab'
			elif'CODESPACE_NAME'in os.environ:return'Github Codespace'
			elif'GITPOD_WORKSPACE_CLUSTER_HOST'in os.environ:return'Gitpod'
			elif'REPLIT_USER'in os.environ:return'Replit'
			elif'KAGGLE_CONTAINER_NAME'in os.environ:return'Kaggle'
			elif A in os.environ and'.hf.space'in os.environ[A]:return'Hugging Face Spaces'
			else:return B
		except KeyError:return B
	def os(C):
		try:
			A=sys.platform
			if A.startswith('linux'):return'Linux'
			elif A=='darwin':return'macOS'
			elif A=='win32':return'Windows'
			else:return'Unknown'
		except Exception as B:return f"Error determining OS: {str(B)}"
lc_init()
def idv():
	A='Không tìm thấy thông tin người dùng hợp lệ. Vui lòng liên hệ Vnstock để được hỗ trợ!';id=PROJECT_DIR/'user.json'
	if not os.path.exists(id):raise SystemExit(A)
	else:
		with open(id,'r')as B:id=json.load(B)
		if not id['user']:raise SystemExit(A)
		return'Valid user!'