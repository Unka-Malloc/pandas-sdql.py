from abc import (
    ABCMeta,
    abstractmethod,
)


class Replaceable(metaclass=ABCMeta):

    @property
    @abstractmethod
    def replaceable(self):
        return

    @property
    @abstractmethod
    def oid(self):
        """
        Object Identifier
        :return:
        """
        return

    @property
    @abstractmethod
    def sdql_ir(self):
        return
