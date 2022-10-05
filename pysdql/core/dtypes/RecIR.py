from pysdql.core.dtypes.SemiRing import SemiRing
from pysdql.core.util.data_interpreter import to_scalar


class RecIR(SemiRing):
    def __init__(self, data: dict, mutable=True):
        self.data = data
        self.mutable = mutable

    def fields(self) -> list:
        return list(self.data.keys())

    def access(self, field):
        if field not in self.fields():
            raise ValueError(f'{field} Not Found')
        else:
            return self.data[field]

    def __getitem__(self, item):
        return self.access(item)

    def __setitem__(self, key, value):
        self.data[key] = value

    @property
    def expr(self) -> str:
        expr_list = []
        if self.mutable:
            for k in self.data.keys():
                expr_list.append(f'"{k}": {self.data[k]}')
            return f"record({{{', '.join(expr_list)}}})"
        else:
            for k in self.data.keys():
                expr_list.append(f'{k}: {to_scalar(self.data[k])}')
            return f"record({{{', '.join(expr_list)}}})"

    def __repr__(self):
        return self.expr