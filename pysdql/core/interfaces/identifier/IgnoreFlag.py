from abc import (
    ABCMeta,
    abstractmethod,
)


class IgnoreThisFlag(metaclass=ABCMeta):
    @property
    def ignore(self):
        return True