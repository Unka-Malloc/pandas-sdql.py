class RecEl:
    def __init__(self, kv_pair: dict):
        self.data = kv_pair

    @property
    def expr(self):
        expr_list = []
        for k in self.data.keys():
            expr_list.append(f'{k}={self.data[k]}')
        return f"< {', '.join(expr_list)} >"

    def __repr__(self):
        return self.expr
