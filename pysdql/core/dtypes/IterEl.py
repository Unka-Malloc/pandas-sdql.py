class IterEl:
    def __init__(self, data):
        self.name = None
        self.kv_pair = None

        if type(data) == str:
            self.name = data
        if type(data) == tuple:
            self.kv_pair = data

    @property
    def key(self) -> str:
        if self.kv_pair:
            return self.kv_pair[0]
        if self.name:
            return f'{self.name}.key'

    @property
    def val(self) -> str:
        if self.kv_pair:
            return self.kv_pair[1]
        if self.name:
            return f'{self.name}.val'

    @property
    def expr(self) -> str:
        if self.kv_pair:
            return f'<{self.key}, {self.val}>'
        if self.name:
            return self.name

    def __repr__(self):
        return self.expr

    def rename(self, data):
        if type(data) == str:
            self.name = data
        if type(data) == tuple:
            self.kv_pair = data

    def dup(self, other) -> bool:
        """
        Detect Duplication Between Iteration Elements
        :return:
        """
        if not type(other) == IterEl:
            raise TypeError('Only detect duplications between IterEl objects')

        if self.expr == other.expr:
            return True
        if self.key == other.key:
            return True
        if self.val == other.val:
            return True

        return False

