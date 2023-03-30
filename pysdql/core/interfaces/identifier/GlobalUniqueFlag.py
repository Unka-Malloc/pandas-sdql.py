from abc import (
    ABCMeta,
    abstractmethod,
)


class GlobalUniqueFlag(metaclass=ABCMeta):

    @property
    @abstractmethod
    def oid(self):
        """
        Object Identifier
        :return:
        """
        return
