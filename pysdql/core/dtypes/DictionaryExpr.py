class DictExpr:
    def __init__(self, data: dict, name=None):
        self.data = data
        self.name = name

    @property
    def expr(self) -> str:
        expr_list = []
        for k in self.data.keys():
            expr_list.append(f'{k} -> {self.data[k]}')
        return f"{{ {', '.join(expr_list)} }}"

    def __repr__(self):
        return self.expr

    def __getitem__(self, item):
        return f'{self.name}({item})'

