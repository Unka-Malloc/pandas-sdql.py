class LoadExpr:
    def __init__(self, dtype: dict, path: str):
        self.dtype = dtype
        self.path = path

        self.names = list(self.dtype.keys())

    @property
    def expr(self) -> str:
        tmp_list = []
        for n in self.dtype.keys():
            tmp_list.append(f'{n}: {self.dtype[n]}')
        tmp_str = ', '.join(tmp_list)
        output = f'load[{{<{tmp_str}> -> int}}]("{self.path}")'
        return output

    def __repr__(self):
        return self.expr
