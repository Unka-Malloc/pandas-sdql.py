import re
from datetime import datetime

from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.IgnoreExpr import IgnoreExpr
from pysdql.core.dtypes.sdql_ir import (
    Expr,
    ConstantExpr,
)


def date_fmt(value) -> int:
    if is_datetime(value):
        date = datetime.strptime(value.replace('"', ''), '%Y-%m-%d %H:%M:%S')
        mo = str(date.month)
        d = str(date.day)
        h = str(date.hour)
        mi = str(date.minute)
        s = str(date.second)
        if len(mo) == 1:
            mo = f'0{mo}'
        if len(d) == 1:
            d = f'0{d}'
        if len(mi) == 1:
            mi = f'0{mi}'
        if len(s) == 1:
            s = f'0{s}'
        return int(f'{date.year}{mo}{d}{h}{mi}{s}')

    if is_date(value):
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

def is_datetime(data):
    if type(data) == str:
        pattern = re.compile(r'(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})')
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
