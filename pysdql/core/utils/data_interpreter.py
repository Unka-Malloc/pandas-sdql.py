import re


def is_date(data: str):
    if type(data) == str:
        reg_exp = r'\d{4}-\d{2}-\d{2}$'
        return re.match(reg_exp, data) is not None


def to_bool(data: bool) -> str:
    if data:
        return 'true'
    else:
        return 'false'


def to_date(data: str) -> str:
    date_list = data.split('-')
    if len(date_list) != 3:
        raise ValueError()
    year = date_list[0]
    month = date_list[1]
    day = date_list[2]
    return f'date({year}{month}{day})'


def to_scalar(data) -> str:
    if type(data) == bool:
        return to_bool(data)
    if type(data) == int:
        return str(data)
    if type(data) == float:
        return str(data)
    if type(data) == str:
        if is_date(data):
            return to_date(data)
        return f'"{data}"'
    return str(data)
