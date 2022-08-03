class DataFrameColumns:
    def __init__(self, df, columns):
        self.df = df
        self.columns = columns

    @property
    def name(self):
        return self.df.name

    @name.setter
    def name(self, val):
        self.df.name = val

    def __len__(self):
        return len(self.columns)

    def __getitem__(self, item):
        return self.columns[item]

    def __setitem__(self, key, value):
        self.columns[key] = value

    def __delitem__(self, key):
        del self.columns[key]

    def __iter__(self):
        return iter(self.columns)

    def __contains__(self, item):
        if item in self.columns:
            return True
        return False

    def __repr__(self):
        return str(self.columns)

    def __add__(self, other):
        return self.columns + other

    def append(self, val):
        self.columns.append(val)