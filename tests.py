import os

import pysdql

from pysdql.core.dtypes.DataFrame import DataFrame
from pysdql.core.dtypes.VarBindExpr import VarBindExpr
from pysdql.core.dtypes.VarBindSeq import VarBindSeq
from pysdql.core.dtypes.sdql_ir import *


def test_VarBindSeq():
    x = VarExpr('x')
    x_val = ConstantExpr(1)

    bind_1 = VarBindExpr(x, x_val)

    y = VarExpr('y')
    y_val = ConstantExpr(2)

    bind_2 = VarBindExpr(y, y_val)

    result = VarExpr('result')
    add_expr = AddExpr(x, y)

    out = VarBindExpr(result, add_expr)

    seq = VarBindSeq()

    seq.push(bind_1, bind_2, out)

    print(seq.sdql_ir)


if __name__ == '__main__':
    # test_VarBindSeq()

    # li = DataFrame()
    # cond = ((li.l_shipdate >= "1994-01-01") &
    #         (li.l_shipdate < "1995-01-01") &
    #         (li.l_discount >= 0.05) &
    #         (li.l_discount <= 0.07) &
    #         (li.l_quantity < 24))
    #
    # print(cond.replace(VarExpr('x')))

    query_obj = pysdql.tpch.q18()

    # print(query_obj.get_retriever().findall_groupby_agg())

    query_obj.show()
