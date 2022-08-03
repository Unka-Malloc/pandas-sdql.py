from pysdql.core.util.type_checker import (
    is_date,
    is_int,
    is_float,
    is_str,
)


class srecord:
    def __init__(self, data, name=None):
        self.data = data
        self.name = name

    @staticmethod
    def from_dict(data: dict):
        expr_list = []
        for k in data.keys():
            v = data[k]
            if is_date(v):
                v = str(v).replace('-', '')
            if is_int(v):
                v = int(v)
            if is_float(v):
                v = float(v)
            if is_str(v):
                v = f'"{v}"'
            expr_list.append(f'{k} = {v}')
        return f"< {', '.join(expr_list)} >"

    @property
    def expr(self) -> str:
        if type(self.data) == dict:
            return self.from_dict(self.data)

    def __repr__(self):
        return self.expr


