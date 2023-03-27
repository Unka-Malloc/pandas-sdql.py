from abc import (
    ABCMeta,
    abstractmethod,
)


class FreeStateExpr(metaclass=ABCMeta):
    @property
    def free(self):
        return True

    @property
    @abstractmethod
    def refer(self):
        return