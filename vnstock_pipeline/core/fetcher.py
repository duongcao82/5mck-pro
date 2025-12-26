import abc
class Fetcher(abc.ABC):
	@abc.abstractmethod
	def fetch(self,ticker:str,**A):0