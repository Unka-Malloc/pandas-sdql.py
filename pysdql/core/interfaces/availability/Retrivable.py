from abc import (
    ABCMeta,
    abstractmethod,
)


class Retrivable(metaclass=ABCMeta):

    @property
    @abstractmethod
    def name(self) -> str:
        return ''

    @property
    @abstractmethod
    def columns(self) -> list:
        return []

    @abstractmethod
    def get_history(self):
        """

        :return: history_object
        """
        return
