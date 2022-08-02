import re
from pysdql.core.util.data_str import (
    remove_prefix,
    remove_suffix,
    remove_sides
)

from pysdql.core.dtypes.RecEl import RecEl
from pysdql.core.dtypes.DictEl import DictEl


def match_int(data):
    """
    Whether data is int type
    :param data:
    :return:
    """
    reg_exp = r'[+-]?[0-9]+$'
    return re.match(reg_exp, data) is not None


def match_real(data):
    """
    Whether data is real type
    :param data:
    :return:
    """
    reg_exp = r'[+-]?[0-9]\.[0-9]+([E][+-]?[0-9]+)?$'
    return re.match(reg_exp, data) is not None


def match_bool(data):
    """
    Whether data is bool type
    :param data:
    :return:
    """
    if data == 'true':
        return True
    if data == 'false':
        return True

    return False


def match_string(data):
    """
    Whether data is string type
    :param data:
    :return:
    """
    reg_exp = r'"[\S\s]*"$'
    return re.match(reg_exp, data) is not None


def match_date(data):
    """
    Whether data is date type
    :param data:
    :return:
    """
    reg_exp = r'DateValue\(\d+\)$'
    return re.match(reg_exp, data) is not None


def match_record(data):
    """
    Whether data is a semi-ring record
    :param data:
    :return:
    """
    reg_exp = r'<([A-Za-z0-9_]+(\s)?=(\s)?\S+(,)?(\s)?)*>$'
    return re.match(reg_exp, data) is not None


def match_dict(data):
    """
    Whether data is a semi-ring dictionary
    :param data:
    :return:
    """
    reg_exp = r'{([\S\s]+(\s)?->(\s)?\S+(,)?(\s)?)*}$'
    return re.match(reg_exp, data) is not None


def from_scalar(expr):
    if match_bool(expr):
        if expr == 'true':
            return True
        if expr == 'false':
            return False
    if match_int(expr):
        return int(expr)
    if match_real(expr):
        return float(expr)
    if match_string(expr):
        return remove_sides(expr, '"')
    if match_date(expr):
        expr = remove_suffix(remove_prefix(expr, 'DateValue('), ')')
        if len(expr) != 8:
            raise ValueError(f'Illegal Date Format: {expr}')
        year = expr[0:4]
        month = expr[4:6]
        day = expr[6:8]
        return f'{year}-{month}-{day}'
    return expr


def from_any(expr):
    if match_bool(expr):
        if expr == 'true':
            return True
        if expr == 'false':
            return False
    if match_int(expr):
        return int(expr)
    if match_real(expr):
        return float(expr)
    if match_string(expr):
        return remove_sides(expr, '"')
    if match_date(expr):
        expr = remove_suffix(remove_prefix(expr, 'DateValue('), ')')
        if len(expr) != 8:
            raise ValueError(f'Illegal Date Format: {expr}')
        year = expr[0:4]
        month = expr[4:6]
        day = expr[6:8]
        return f'{year}-{month}-{day}'
    if match_record(expr):
        return from_record(expr)
    return expr


def from_record(expr):
    expr_dict = {}
    expr = remove_prefix(remove_suffix(expr, '>'), '<')
    reg_exp = r'\w+\s*=\s*\S+\s*'
    expr_list = re.findall(reg_exp, expr)
    for e in expr_list:
        e = remove_suffix(e.strip(), ',')
        key_val_pair = e.strip().split('=')

        if len(key_val_pair) != 2:
            raise TypeError('Match RecEl Failed!')

        key = from_scalar(key_val_pair[0].strip())
        val = from_scalar(key_val_pair[1].strip())

        expr_dict[key] = val

    return RecEl(expr_dict)


def from_dict(expr):
    expr_dict = {}
    expr = remove_prefix(remove_suffix(expr, '}'), '{')
    reg_exp = r'[\S\s]+\s*->\s*[\S\s]+\s*'
    expr_list = re.findall(reg_exp, expr)
    for e in expr_list:
        e = remove_suffix(e.strip(), ',')
        key_val_pair = e.strip().split('->')

        if len(key_val_pair) != 2:
            raise TypeError('Match RecEl Failed!')

        key = from_any(key_val_pair[0].strip())
        val = from_any(key_val_pair[1].strip())

        expr_dict[key] = val

    return DictEl(expr_dict)


def from_expr(expr: str):
    if match_bool(expr):
        if expr == 'true':
            return True
        if expr == 'false':
            return False
    if match_int(expr):
        return int(expr)
    if match_real(expr):
        return float(expr)
    if match_string(expr):
        return remove_sides(expr, '"')
    if match_record(expr):
        return from_record(expr)
    if match_dict(expr):
        return from_dict(expr)

    raise ValueError(f'Match Expression Failed: {expr}')
