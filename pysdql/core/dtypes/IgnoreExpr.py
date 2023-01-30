from abc import (
    ABCMeta,
    abstractmethod,
)


class IgnoreExpr(metaclass=ABCMeta):
    @property
    def ignore(self):
        return True