from pysdql.core.dtypes.VarExpr import VarExpr


class ArrayExpr:
    def __init__(self, name: str, data: list):
        self.name = name
        self.data = data

    @property
    def expr(self):
        return f'{self.name}'

    def __repr__(self):
        return self.expr

    def __getitem__(self, item):
        iname = f'val_{item}'
        idata = f'{self.name}({item})'
        print(f'let {iname} = {idata}')
        return VarExpr(name=iname, data=idata)
