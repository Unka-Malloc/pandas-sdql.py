import re


def is_neg_int(data: str):
    if data[0] == '-':
        return True

    return False


def has_dot(data: str):
    if '.' in data:
        return True

    return False


def rmv_neg(data: str):
    return data.replace('-', '')


def rmv_dot(data: str):
    return data.replace('.', '')


def is_date(data):
    if type(data) == str:
        data = str(data)
        pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
        if pattern.findall(data.strip()):
            return True
    return False


def is_int(data):
    if type(data) == int:
        return True

    if type(data) == str:
        data = str(data)

        if is_date(data):
            return False

        if is_neg_int(data):
            data = rmv_neg(data)
        if data.isdigit():
            return True

    return False


def is_float(data):
    if type(data) == float:
        return True

    if type(data) == str:
        data = str(data)

        if is_int(data):
            return False

        if is_neg_int(data):
            data = rmv_neg(data)
        if has_dot(data):
            data = rmv_dot(data)
        if data.isdigit():
            return True

    return False


def is_str(data):
    if is_date(data):
        return False
    if is_int(data):
        return False
    if is_float(data):
        return False

    if type(data) == str:
        return True

    return False


def is_scalar(data):
    if is_str(data):
        return True
    if is_int(data):
        return True
    if is_float(data):
        return True

    return False
