class IterEl:
    def __init__(self, data):
        self.__name = None
        self.__key_val = None

        if type(data) == str:
            self.__name = data
        elif type(data) == tuple:
            self.__key_val = data
        else:
            raise ValueError('Only accept "<k, v>" or "x"')

    @property
    def key(self) -> str:
        if self.__key_val:
            return self.__key_val[0]
        if self.__name:
            return f'{self.__name}_k'

    @property
    def val(self) -> str:
        if self.__key_val:
            return self.__key_val[1]
        if self.__name:
            return f'{self.__name}_v'

    @property
    def expr(self) -> str:
        return f'<{self.key}, {self.val}>'

    def __repr__(self):
        return self.expr

    def rename(self, data):
        if type(data) == str:
            self.__name = data
        if type(data) == tuple:
            self.__key_val = data

    def dup(self, other) -> bool:
        """
        Detect Duplication Between Iteration Elements
        :return:
        """
        if type(other) != IterEl:
            raise TypeError('Only detect duplications between IterEl objects')

        if self.expr == other.expr:
            return True
        if self.key == other.key:
            return True
        if self.val == other.val:
            return True

        return False

