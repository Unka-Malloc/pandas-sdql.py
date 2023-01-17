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


def merge(left, right, how='inner', left_on=None, right_on=None):
    return left.merge(right, how=how, left_on=left_on, right_on=right_on)


# def merge(*args, on=None, name='R', optimize=False, by_cols=False, by_relation=False):
#     # if by_relation:
#     #     args = list(args)
#     #     args.reverse()
#     #     r = args.pop()
#     #     s = args.pop()
#     #     r_merged = r.merge(s, on)
#     #     args.reverse()
#     #     for t in args:
#     #         r_merged = r_merged.merge(t, on)
#     #     return r_merged
#
#     ie_list = []
#     ik_list = []
#     iv_list = []
#     op_list = []
#     col_list = []
#     icol_list = []
#
#     for r in args:
#         ie_list.append(str(r.iter_expr))
#         ik_list.append(str(r.iter_expr.key))
#         iv_list.append(str(r.iter_expr.val))
#         icol_list.append(r.cols)
#
#         col_list += r.cols
#         op_list += r.operations
#
#     if by_cols:
#         if on:
#             result = VarExpr(name, IterStmt(ie_list, CondStmt(on, DictEl({concat_cols(ik_list, icol_list): 1}),
#                                                               DictEl({}))))
#             op_list.append(OpExpr('pysdql_merge_on_by_cols', result))
#         else:
#             result = VarExpr(name, IterStmt(ie_list, DictEl({concat_cols(ik_list, icol_list): 1})))
#             op_list.append(OpExpr('pysdql_merge_by_cols', result))
#     else:
#         iv_str = " * ".join(iv_list)
#         if on:
#             result = VarExpr(name, IterStmt(ie_list, CondStmt(on, DictEl({concat(ik_list): iv_str}), DictEl({}))))
#             op_list.append(OpExpr('pysdql_merge_on_by_concatexpr', result))
#         else:
#             result = VarExpr(name, IterStmt(ie_list, DictEl({concat(ik_list): iv_str})))
#             op_list.append(OpExpr('pysdql_merge_by_concatexpr', result))
#
#     return relation(name=name,
#                     cols=col_list,
#                     operations=op_list)


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
