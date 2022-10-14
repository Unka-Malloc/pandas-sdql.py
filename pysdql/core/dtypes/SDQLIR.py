from abc import (
    ABCMeta,
    abstractmethod,
)


class SDQLIR(metaclass=ABCMeta):

    @property
    @abstractmethod
    def sdql_ir(self):
        pass
