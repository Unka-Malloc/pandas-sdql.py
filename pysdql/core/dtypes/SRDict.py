import os


class sdict:
    def __init__(self, data, name=None):
        self.data = data
        self.name = name

    @staticmethod
    def from_dict(data: dict):
        expr_list = []
        for k in data.keys():
            expr_list.append(f'{k} -> {data[k]}')
        sep = f', \n'
        return f"{{ {sep.join(expr_list)} }}"

    @staticmethod
    def from_list(data: list or tuple):
        print(data)

    @staticmethod
    def from_set(data: set):
        print(data)

    def expr(self):
        if type(self.data) == list or type(self.data) == tuple:
            return self.from_list(self.data)
        if type(self.data) == dict:
            return self.from_dict(self.data)

    def __repr__(self):
        return self.expr()
