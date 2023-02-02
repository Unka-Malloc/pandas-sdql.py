import re
from datetime import datetime

from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.IgnoreExpr import IgnoreExpr
from pysdql.core.dtypes.sdql_ir import (
    Expr,
    ConstantExpr,
)


def date_fmt(value) -> int:
    date = datetime.strptime(value.replace('"', ''), '%Y-%m-%d')
    m = str(date.month)
    d = str(date.day)
    if len(m) == 1:
        m = f'0{m}'
    if len(d) == 1:
        d = f'0{d}'
    return int(f'{date.year}{m}{d}')


def is_date(data) -> bool:
    if type(data) == str:
        pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
        if pattern.findall(data.strip()):
            return True
    return False


def input_fmt(data):
    # print(data, type(data))
    if is_date(data):
        return ConstantExpr(date_fmt(data))
    elif type(data) in (bool, int, float, str):
        return ConstantExpr(data)
    elif isinstance(data, Expr):
        return data
    elif isinstance(data, FlexIR):
        return data.sdql_ir
    elif isinstance(data, IgnoreExpr):
        return ConstantExpr(True)
    else:
        raise TypeError(f'Unsupport type {type(data)}')
