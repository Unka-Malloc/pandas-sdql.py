from abc import (
    ABCMeta,
    abstractmethod,
)


class Flexible(metaclass=ABCMeta):

    @property
    @abstractmethod
    def expr(self) -> str:
        pass
