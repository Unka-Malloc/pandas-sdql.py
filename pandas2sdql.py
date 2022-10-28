import base64

from pysdql import DataFrame

from pysdql.core.dtypes.sdql_ir import (
    PrintAST,
    GenerateSDQLCode, ConstantExpr, LetExpr,
)

import pysdql as pd


def q1(li):
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

    result.show()

    return result.optimize()


def q3(cu, ord, li):
    cu_filt = cu[cu.c_mktsegment == "BUILDING"]
    cu_filt = cu_filt[["c_custkey"]]

    ord_filt = ord[ord.o_orderdate < "1995-03-15"]
    ord_cu_join = pd.merge(cu_filt, ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")
    ord_cu_join = ord_cu_join[["o_orderkey", "o_orderdate", "o_shippriority"]]

    li_filt = li[li.l_shipdate > "1995-03-15"]
    li_order_join = pd.merge(ord_cu_join, li_filt, left_on="o_orderkey", right_on="l_orderkey", how="inner")
    li_order_join["revenue"] = li_order_join.l_extendedprice * (1 - li_order_join.l_discount)

    result = li_order_join \
        .groupby(["l_orderkey", "o_orderdate", "o_shippriority"]) \
        .agg(revenue=("revenue", "sum"))

    # print('>> cu_filt operations <<')
    # print(cu_filt.operations)
    #
    # print('>> cu_ord_join operations <<')
    # print(ord_cu_join.operations)
    #
    # print('>> li_filt operations <<')
    # print(li_filt.operations)

    # result.show()

    return result.optimize()


def q6(li):
    li_filt = li[
        (li.l_shipdate >= "1994-01-01") &
        (li.l_shipdate < "1995-01-01") &
        (li.l_discount >= 0.05) &
        (li.l_discount <= 0.07) &
        (li.l_quantity < 24)
        ]

    li_filt['revenue'] = li_filt.l_extendedprice * li_filt.l_discount

    result = li_filt.agg({'revenue': 'sum'})

    result.show()

    return result.optimize()


def q10(ord, cu, na, li):
    ord_filt = ord[(ord.o_orderdate >= "1993-10-01") & (ord.o_orderdate < "1994-01-01")]

    cu_proj = cu[["c_custkey", "c_name", "c_acctbal", "c_address", "c_nationkey", "c_phone", "c_comment"]]
    ord_cu_join = pd.merge(cu_proj, ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")

    na_proj = na[["n_nationkey", "n_name"]]
    ord_na_join = pd.merge(na_proj, ord_cu_join, left_on="n_nationkey", right_on="c_nationkey", how="inner")
    ord_na_join = ord_na_join[["o_orderkey", "c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"]]

    li_filt = li[(li.l_returnflag == "R")]

    li_ord_join = pd.merge(ord_na_join, li_filt, left_on="o_orderkey", right_on="l_orderkey", how="inner")

    li_ord_join["revenue"] = li_ord_join.l_extendedprice * (1 - li_ord_join.l_discount)

    result = li_ord_join \
        .groupby(["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"]) \
        .agg(sum_revenue=("revenue", "sum"))

    return result.optimize()


def q19():
    pa = DataFrame()
    li = DataFrame()

    pa_filt = pa[
        ((pa.p_brand == "Brand#12")
         & (pa.p_container.isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"]))
         & (pa.p_size >= 1)
         & (pa.p_size <= 5)) |
        ((pa.p_brand == "Brand#23")
         & (pa.p_container.isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"]))
         & (pa.p_size >= 1)
         & (pa.p_size <= 10)) |
        ((pa.p_brand == "Brand#34")
         & (pa.p_container.isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"]))
         & (pa.p_size >= 1) & (pa.p_size <= 15))
        ]

    pa_proj = pa_filt[["p_partkey", "p_brand", "p_size", "p_container"]]

    li_filt = li[(((li.l_shipmode == "AIR") | (li.l_shipmode == "AIR REG"))
                  & (li.l_shipinstruct == "DELIVER IN PERSON"))]
    li_pa_join = pd.merge(pa_proj, li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")
    li_pa_join_filt = li_pa_join[
        (
                ((li_pa_join.p_brand == "Brand#12") & ((li_pa_join.l_quantity >= 1) & (li_pa_join.l_quantity <= 11))) |
                ((li_pa_join.p_brand == "Brand#23") & ((li_pa_join.l_quantity >= 10) & (li_pa_join.l_quantity <= 20))) |
                ((li_pa_join.p_brand == "Brand#34") & ((li_pa_join.l_quantity >= 20) & (li_pa_join.l_quantity <= 30)))
        )
    ]

    print(li_pa_join_filt.operations)
    print(li_pa_join_filt.optimize())

    # li_pa_join_filt["revenue"] = li_pa_join_filt.l_extendedprice * (1 - li_pa_join_filt.l_discount)
    # result = li_pa_join_filt.revenue.sum()

    # print(result.operations)
    # print(result.optimize())

    # return result


if __name__ == '__main__':
    ord = DataFrame()
    cu = DataFrame()
    na = DataFrame()
    li = DataFrame()

    # q1(li)
    q3(cu, ord, li)
    # q6(li)
    # q10(ord, cu, na, li)
    # q19()

    # li = DataFrame()
    # PrintAST((li.l_shipdate >= "1994-01-01") &
    # (li.l_shipdate < "1995-01-01") &
    # (li.l_discount >= 0.05) &
    # (li.l_discount <= 0.07) &
    # (li.l_quantity < 24))
