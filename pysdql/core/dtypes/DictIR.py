from pysdql.core.dtypes.SemiRing import SemiRing
from pysdql.core.util.data_interpreter import to_scalar


class DictIR(SemiRing):
    def __init__(self, data: dict, mutable=True):
        self.data = data
        self.mutable = mutable

    @property
    def expr(self) -> str:
        expr_list = []
        if self.mutable:
            for k in self.data.keys():
                expr_list.append(f'{k}: {self.data[k]}')
        else:
            for k in self.data.keys():
                expr_list.append(f'{to_scalar(k)}: {to_scalar(self.data[k])}')
        return f"{{{', '.join(expr_list)}}}"

    def __repr__(self):
        return self.expr
