class Series:
    def __init__(self, data=None, index=None, dtype=None, name=None, fastpath=False):
        self.data = data
        self.index = index
        self.dtype = dtype
        self.name = name
        self.fastpath = fastpath