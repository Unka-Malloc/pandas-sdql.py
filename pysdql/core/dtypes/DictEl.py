class DictEl:
    def __init__(self, data: dict):
        self.data = data

    @property
    def expr(self) -> str:
        expr_list = []
        for k in self.data.keys():
            expr_list.append(f'{k} -> {self.data[k]}')
        return f"{{ {', '.join(expr_list)} }}"

    def __repr__(self):
        return self.expr

