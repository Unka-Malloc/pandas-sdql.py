from abc import (
    ABCMeta,
    abstractmethod,
)


class SemiRing(metaclass=ABCMeta):

    @property
    @abstractmethod
    def expr(self) -> str:
        pass
