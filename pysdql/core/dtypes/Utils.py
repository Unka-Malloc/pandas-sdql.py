import re
from datetime import datetime

from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.IgnoreExpr import IgnoreExpr
from pysdql.core.dtypes.FreeStateExpr import FreeStateExpr
from pysdql.core.dtypes.sdql_ir import (
    Expr,
    VarExpr,
    ConstantExpr,
)


def date_fmt(value) -> int:
    if is_datetime(value):
        date = datetime.strptime(value.replace('"', ''), '%Y-%m-%d %H:%M:%S')
        mo = str(date.month)
        this_day = str(date.day)
        this_hour = str(date.hour)
        this_minute = str(date.minute)
        this_sec = str(date.second)
        if len(mo) == 1:
            mo = f'0{mo}'
        if len(this_day) == 1:
            this_day = f'0{this_day}'

        if len(this_minute) == 1:
            this_minute = f'0{this_minute}'
        if len(this_sec) == 1:
            this_sec = f'0{this_sec}'
        return int(f'{date.year}{mo}{this_day}')

    if is_date(value):
        date = datetime.strptime(value.replace('"', ''), '%Y-%m-%d')
        m = str(date.month)
        this_day = str(date.day)
        if len(m) == 1:
            m = f'0{m}'
        if len(this_day) == 1:
            this_day = f'0{this_day}'
        return int(f'{date.year}{m}{this_day}')


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
    elif isinstance(data, FreeStateExpr):
        return VarExpr(data.refer)
    else:
        raise TypeError(f'Unsupport type {type(data)}')
