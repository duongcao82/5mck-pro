import abc
class Transformer(abc.ABC):
	@abc.abstractmethod
	def transform(self,data):0