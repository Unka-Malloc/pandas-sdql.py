import base64

from pysdql import DataFrame

from pysdql.core.dtypes.sdql_ir import (
    PrintAST,
    GenerateSDQLCode,
)

import pysdql as pd


def q1():
    li = DataFrame()

    li_filt = li[(li.l_shipdate <= "1998-09-02")]
    li_filt["disc_price"] = li_filt.l_extendedprice * (1 - li_filt.l_discount)
    li_filt["charge"] = li_filt.l_extendedprice * (1 - li_filt.l_discount) * (1 + li_filt.l_tax)

    result = li_filt \
        .groupby(["l_returnflag", "l_linestatus"]) \
        .agg(sum_qty=("l_quantity", "sum"),
             sum_base_price=("l_extendedprice", "sum"),
             sum_disc_price=("disc_price", "sum"),
             sum_charge=("charge", "sum"),
             count_order=("l_quantity", "count")
             )

    print(result.operations)

    print(result.optimize())
    PrintAST(result.optimize())

    print(GenerateSDQLCode(result.optimize()))


def q3():
    cu = DataFrame()
    ord = DataFrame()
    li = DataFrame()

    cu_filt = cu[cu.c_mktsegment == "BUILDING"]
    cu_filt = cu_filt[["c_custkey"]]

    # print(cu_filt.operations)

    ord_filt = ord[ord.o_orderdate < "1995-03-15"]
    ord_cu_join = pd.merge(cu_filt, ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")
    ord_cu_join = ord_cu_join[["o_orderkey", "o_orderdate", "o_shippriority"]]

    # li_filt = li[li.l_shipdate > "1995-03-15"]
    # li_order_join = pd.merge(ord_cu_join, li_filt, left_on="o_orderkey", right_on="l_orderkey", how="inner")
    # li_order_join["revenue"] = li_order_join.l_extendedprice * (1 - li_order_join.l_discount)
    #
    # result = li_order_join \
    #     .groupby(["l_orderkey", "o_orderdate", "o_shippriority"]) \
    #     .agg(revenue=("revenue", "sum"))

    # return result

    print(ord_cu_join.operations)

    print(ord_cu_join.merge_right_stmt(None))


def q6():
    # replaced by read_csv() in the future,
    # the name of the Dataframe will be set to the name of the csv file by default
    li = DataFrame()

    li_filt = li[
        (li.l_shipdate >= "1994-01-01") &
        (li.l_shipdate < "1995-01-01") &
        (li.l_discount >= 0.05) &
        (li.l_discount <= 0.07) &
        (li.l_quantity < 24)
        ]

    result = (li_filt.l_extendedprice * li_filt.l_discount).sum()

    print(result.operations)
    print(result.optimize())
    PrintAST(result.optimize())

    print(GenerateSDQLCode(result.optimize()))

    return result


if __name__ == '__main__':
    # q1()
    # q3()
    q6()

    # li = DataFrame()
    # PrintAST((li.l_shipdate >= "1994-01-01") &
    # (li.l_shipdate < "1995-01-01") &
    # (li.l_discount >= 0.05) &
    # (li.l_discount <= 0.07) &
    # (li.l_quantity < 24))
