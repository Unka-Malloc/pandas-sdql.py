from pysdql.core.api import (
    # db_driver
    db_driver,

    # dtypes
    DataFrame,

    relation,

    IterStmt,
    CondStmt,
    DictEl,
    RecEl,
    VarExpr,
    OpExpr,
    ConcatExpr,

    # data_loader
    read_csv,
    # read_tbl,
    # read_table,
    tune_tbl,
    # load_tbl,

    # data_parser
    get_tbl_type,
    get_load,
)
from pysdql.const import (
    PART_COLS,
    SUPPLIER_COLS,
    PARTSUPP_COLS,
    CUSTOMER_COLS,
    ORDERS_COLS,
    LINEITEM_COLS,
    NATION_COLS,
    REGION_COLS,

    PART_LOAD,
    SUPPLIER_LOAD,
    PARTSUPP_LOAD,
    CUSTOMER_LOAD,
    ORDERS_LOAD,
    LINEITEM_LOAD,
    NATION_LOAD,
    REGION_LOAD,
)
from pysdql.core.dtypes.ExtDateTime import ExtDatetime
from pysdql.core.dtypes.ExternalExpr import ExternalExpr
from pysdql.core.dtypes.sdql_ir import ExtFuncExpr, ExtFuncSymbol, RecAccessExpr, PairAccessExpr, ConstantExpr

from pysdql.core.query import *

from pysdql.query import (
    tpch
)

from pysdql.query.tpch import tpch_query


def merge(left, right, how='inner', left_on=None, right_on=None):
    return left.merge(right, how=how, left_on=left_on, right_on=right_on)


def concat(keys: list) -> str:
    keys.reverse()
    k1 = keys.pop()
    k2 = keys.pop()
    ce = ConcatExpr(rec1=k1, rec2=k2)
    keys.reverse()
    for k in keys:
        ce = ce.concat(k)
    return str(ce)


def concat_cols(keys: list, cols: list):
    cols_dict = dict(zip(keys, cols))
    tmp_dict = {}

    for k in cols_dict.keys():
        for c in cols_dict[k]:
            tmp_dict[f'{c}'] = f'{k}.{c}'

    return RecEl(tmp_dict)


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
    return ExtDatetime(col)
