import inspect,importlib,pkgutil
from abc import ABC
from functools import wraps
from tenacity import retry,stop_after_attempt,wait_exponential
from vnstock_data.config import Config
def dynamic_method(func):
	A=func.__name__
	@wraps(func)
	def B(self,*D,**E):
		B=self
		if not hasattr(B._provider,A):raise NotImplementedError(f"Source '{B.source}' does not support '{A}'")
		C=getattr(B._provider,A);F=inspect.signature(C);G={A:B for(A,B)in E.items()if A in F.parameters};return C(*D,**G)
	return B
class BaseAdapter(ABC):
	_module_name:str
	def __init__(A,source:str,symbol:str=None,**O):
		N='symbol';C=symbol;B=source;A.source=B;A.symbol=C;import vnstock_data.explorer as F;G={A.name for A in pkgutil.iter_modules(F.__path__)};H={A for A in G if A.lower()==B.lower()}
		if not H:raise ValueError(f"No data-source '{B}' found. Available: {sorted(G)}")
		I=H.pop();J=f"{F.__name__}.{I}.{A._module_name}"
		try:P=importlib.import_module(J)
		except ImportError as D:raise ValueError(f"Source '{I}' does not provide '{A._module_name}'")from D
		try:K=getattr(P,A.__class__.__name__)
		except AttributeError as D:raise ValueError(f"Module '{J}' has no class '{A.__class__.__name__}'")from D
		L=inspect.signature(K.__init__);E={}
		if C is not None and N in L.parameters:E[N]=C
		for(M,Q)in O.items():
			if M in L.parameters:E[M]=Q
		A._provider=K(**E)
	def __getattr__(A,name):return getattr(A._provider,name)
	@retry(stop=stop_after_attempt(Config.RETRIES),wait=wait_exponential(multiplier=Config.BACKOFF_MULTIPLIER,min=Config.BACKOFF_MIN,max=Config.BACKOFF_MAX))
	def history(self,*A,**B):return self._provider.history(*A,**B)