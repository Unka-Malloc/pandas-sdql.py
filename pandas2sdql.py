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
             count_order=("l_quantity", "count"))

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

    result.show()

    return result.optimize()


def q4(li, ord):
    li_filt = li[li.l_commitdate < li.l_receiptdate]
    li_proj = li_filt[["l_orderkey"]]

    ord_filt = ord[(ord.o_orderdate >= "1993-07-01")
                   & (ord.o_orderdate < "1993-10-01")
                   & ord.o_orderkey.isin(li_proj["l_orderkey"])]

    results = ord_filt \
        .groupby(["o_orderpriority"]) \
        .agg(order_count=("o_orderdate", "count"))

    results.show()

    return results.optimize()


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
    ord_na_join = ord_na_join[
        ["o_orderkey", "c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"]]

    li_filt = li[(li.l_returnflag == "R")]

    li_ord_join = pd.merge(ord_na_join, li_filt, left_on="o_orderkey", right_on="l_orderkey", how="inner")

    li_ord_join["revenue"] = li_ord_join.l_extendedprice * (1 - li_ord_join.l_discount)

    result = li_ord_join \
        .groupby(["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"]) \
        .agg(revenue=("revenue", "sum"))

    result.show()

    return result.optimize()


def q14(li, pa):
    # # pa_filt = pa[pa.p_type.str.startswith("PROMO")]
    # # pa_filt = pa_filt.set_index(["p_partkey"])

    # # li_filt = li[(lineitem.l_shipdate >= "1995-09-01") & (li.l_shipdate < "1995-10-01")]

    # # li_filt["A"] = li_filt.apply(lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) if x["l_partkey"] in pa_filt.index else 0, axis=1)
    # # li_filt["B"] = li_filt.l_extendedprice * (1 - li_filt.l_discount)

    # # result = 100 * li_filt.A.sum() / li_filt.B.sum()
    # # return result

    li_filt = li[(lineitem.l_shipdate >= "1995-09-01") & (li.l_shipdate < "1995-10-01")]
    pa_proj = pa[["p_partkey", "p_type"]]
    li_pa_join = pd.merge(pa_proj, li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")
    li_pa_join["A"] = li_pa_join.apply(
        lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) if x["p_type"].startswith("PROMO") else 0, axis=1)
    li_pa_join["B"] = li_pa_join.l_extendedprice * (1 - li_pa_join.l_discount)

    result = li_pa_join.A.sum() / li_pa_join.B.sum() * 100

    result.show()

    return result.optimize()


def q15(li, su):
    li_filt = li[(li.l_shipdate >= "1996-01-01") & (li.l_shipdate < "1996-04-01")]
    li_filt["revenue"] = li_filt.l_extendedprice * (1 - li_filt.l_discount)

    li_aggr = li_filt \
        .groupby(["l_suppkey"]) \
        .agg(total_revenue=("revenue", "sum"))
    li_aggr = li_aggr[li_aggr.total_revenue == 1772627.2087]

    su_proj = su[["s_suppkey", "s_name", "s_address", "s_phone"]]
    li_su_join = pd.merge(su_proj, li_aggr, left_on="s_suppkey", right_on="l_suppkey", how="inner")

    result = li_su_join[["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]]

    result.show()

    return result.optimize()


def q16_pandas(ps, pa, su):
    pa_filt = pa[
        (pa.p_brand != "Brand#45") &
        (pa.p_type.str.startswith("MEDIUM POLISHED") == False) &
        (
                (pa.p_size == 49) |
                (pa.p_size == 14) |
                (pa.p_size == 23) |
                (pa.p_size == 45) |
                (pa.p_size == 19) |
                (pa.p_size == 3) |
                (pa.p_size == 36) |
                (pa.p_size == 9)
        )]
    pa_proj = pa_filt[["p_partkey", "p_brand", "p_type", "p_size"]]

    su_filt = su[
        su.s_comment.str.contains("Customer") & (su.s_comment.str.find("Customer") + 7) < su.s_comment.str.find(
            "Complaints")]
    su_proj = su_filt[["s_suppkey"]]

    ps_filt = ps[~ps.ps_suppkey.isin(su_proj["s_suppkey"])]

    ps_pa_join = pd.merge(pa_proj, ps_filt, left_on="p_partkey", right_on="ps_partkey", how="inner")

    result = ps_pa_join \
        .groupby(["p_brand", "p_type", "p_size"]) \
        .agg(supplier_cnt=("ps_suppkey", lambda x: x.nunique()))

    result.show()

    return result.optimize()


def q19(pa, li):
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

    li_pa_join_filt["revenue"] = li_pa_join_filt.l_extendedprice * (1 - li_pa_join_filt.l_discount)

    result = li_pa_join_filt.agg({'revenue': 'sum'})

    result.show()

    return result.optimize()


if __name__ == '__main__':
    ord = DataFrame()
    cu = DataFrame()
    na = DataFrame()
    li = DataFrame()
    pa = DataFrame()
    su = DataFrame()

    # q1(li)
    # q4(li, ord)
    # q3(cu, ord, li)
    # q6(li)
    # q10(ord, cu, na, li)
    # q19(pa, li)
    q15(li, su)
