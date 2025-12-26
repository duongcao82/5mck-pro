import abc
class Validator(abc.ABC):
	@abc.abstractmethod
	def validate(self,data):0