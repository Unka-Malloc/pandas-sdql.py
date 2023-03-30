from pysdql.const import (
    PART_COLS,
    SUPPLIER_COLS,
    PARTSUPP_COLS,
    CUSTOMER_COLS,
    ORDERS_COLS,
    LINEITEM_COLS,
    NATION_COLS,
    REGION_COLS,
)

from pysdql.core.api import (
    read_csv,
)

from pysdql.core.dtypes.DataFrame import DataFrame

from pysdql.core.SDQLWrapper import tosdql

from pysdql.query.tpch import tpch_query

from pysdql.config import (
    set_verify,
    get_config,
    get_pysdql_path,
    is_pandas_available,
    is_duckdb_available,
)

def merge(left, right, how='inner', left_on=None, right_on=None):
    return left.merge(right, how=how, left_on=left_on, right_on=right_on)


def month(date: str, m: int):
    date_list = date.split('-')
    old_m = int(date_list[1])
    new_m = old_m + m
    if len(str(new_m)) == 1:
        date_list[1] = f'0{new_m}'
    else:
        date_list[1] = f'{new_m}'
    print(f'{"-".join(date_list)}')
    return f'{"-".join(date_list)}'


def set_option(*args):
    return


def DatetimeIndex(col):
    from pysdql.core.exprs.carrier.api import DateTimeProperty

    return DateTimeProperty(col)
