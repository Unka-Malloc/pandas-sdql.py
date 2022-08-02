from pysdql.core.dtypes.SemiRing import SemiRing
from pysdql.core.util.data_interpreter import to_scalar


class DictEl(SemiRing):
    def __init__(self, data: dict):
        self.data = data

    @property
    def expr(self) -> str:
        expr_list = []
        for k in self.data.keys():
            expr_list.append(f'{to_scalar(k)} -> {to_scalar(self.data[k])}')
        return f"{{{', '.join(expr_list)}}}"

    def __repr__(self):
        return self.expr
