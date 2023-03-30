from abc import (
    ABCMeta,
    abstractmethod,
)


class FreeState(metaclass=ABCMeta):
    @property
    def free(self):
        return True

    @property
    @abstractmethod
    def refer(self):
        return