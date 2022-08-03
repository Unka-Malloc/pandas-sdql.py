import re


def is_int(data):
    if type(data) == int:
        return True

    if type(data) == str:
        data = str(data).strip()
        reg_exp = r'[+-]?[0-9]+$'
        return re.match(reg_exp, data) is not None

    return False


def is_float(data):
    if type(data) == float:
        return True

    if type(data) == str:
        data = str(data).strip()
        reg_exp = r'[+-]?[0-9]+\.[0-9]+([E][+-]?[0-9]+)?$'
        return re.match(reg_exp, data) is not None

    return False


def is_date(data):
    if type(data) == str:
        data = str(data).strip()
        reg_exp = r'\d{4}-\d{2}-\d{2}$'
        return re.match(reg_exp, data) is not None
    return False


def is_str(data):
    if is_int(data):
        return False
    if is_float(data):
        return False
    if is_date(data):
        return False

    if type(data) == str:
        return True

    return False


def is_scalar(data):
    if is_int(data):
        return True
    if is_float(data):
        return True
    if is_date(data):
        return True
    if is_str(data):
        return True

    return False


def is_name(data):
    if type(data) == str:
        data = str(data).strip()
        reg_exp = r'[A-Za-z0-9_]$'
        return re.match(reg_exp, data) is not None
    return False


def is_header(data: list):
    for i in data:
        if not is_name(i):
            return False
    return True
