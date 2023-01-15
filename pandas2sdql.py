from pysdql.core.dtypes.sdql_ir import (
    PrintAST,
    GenerateSDQLCode,
)

import pysdql as pd

from pysdql import DataFrame


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


def q2(pa, su, ps, na, re):
    raise NotImplementedError

    re_filt = re[re['r_name'] == 'EUROPE']

    re_na_join = pd.merge(left=re_filt, right=na, left_on='r_regionkey', right_on='n_regionkey', how='inner')
    re_na_join = re_na_join[['n_nationkey']]

    na_su_join = pd.merge(left=re_na_join, right=su, left_on='n_nationkey', right_on='s_nationkey', how='inner')
    na_su_join = na_su_join[['s_suppkey']]

    pa_filt = pa[(pa['p_size'] == 14) & (pa['p_type'].str.endswith('BRASS'))]

    su_ps_join = pd.merge(left=na_su_join, right=ps, left_on='s_suppkey', right_on='ps_suppkey', how='inner')
    su_ps_join = su_ps_join[['ps_suppkey']]

    ps_pa_join = pd.merge(left=su_ps_join, right=pa_filt, left_on='ps_partkey', right_on='p_partkey', how='inner')

    result = ps_pa_join[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]

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


def q5(cu, ord, li, su, na, re):
    re_filt = re[re['r_name'] == 'MIDDLE EAST']

    re_na_join = pd.merge(left=re_filt, right=na, left_on='r_regionkey', right_on='n_regionkey')

    na_cu_join = pd.merge(left=re_na_join, right=cu, left_on='n_nationkey', right_on='c_nationkey')

    ord_filt = ord[(ord['o_orderdate'] >= '1995-01-01') & (ord['o_orderdate'] < '1996-01-01')]

    cu_ord_join = pd.merge(left=na_cu_join, right=ord_filt, left_on='c_custkey', right_on='o_custkey')

    ord_li_join = pd.merge(left=cu_ord_join, right=li, left_on='o_orderkey', right_on='l_orderkey')

    su_ord_li_join = pd.merge(left=su,
                              right=ord_li_join,
                              left_on=['s_suppkey', 's_nationkey'],
                              right_on=['l_suppkey', 'c_nationkey'])

    su_ord_li_join['revenue'] = su_ord_li_join['l_extendedprice'] * (1 - su_ord_li_join['l_discount'])

    result = su_ord_li_join.groupby(['n_name'], as_index=False).agg(revenue=('revenue', 'sum'))

    result.show()

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


def q7(su, li, ord, cu, n1, n2):
    n1_filt = n1[(n1['n_name_1'] == 'FRANCE') | (n1['n_name_1'] == 'GERMANY')]

    n2_filt = n2[(n2['n_name_2'] == 'GERMANY') | (n2['n_name_2'] == 'FRANCE')]

    n1_n2_join = n1_filt.merge(n2_filt, how='cross')

    n1_n2_join = n1_n2_join[(n1['n_name_1'] == 'FRANCE') & (n2['n_name_2'] == 'GERMANY')
                            | ((n1['n_name_1'] == 'GERMANY') & (n2['n_name_2'] == 'FRANCE'))]

    n1_n2_cu_join = n1_n2_join.merge(cu, left_on='n_nationkey_2', right_on='c_nationkey')
    n1_n2_cu_join = n1_n2_cu_join[['n_name_1', 'n_name_2', 'n_nationkey_1', 'c_custkey']]

    n1_n2_cu_su_join = n1_n2_cu_join.merge(su, left_on='n_nationkey_1', right_on='s_nationkey')
    n1_n2_cu_su_join = n1_n2_cu_su_join[['n_name_1', 'n_name_2', 'c_custkey', 's_suppkey']]

    n1_n2_cu_su_ord_join = n1_n2_cu_su_join.merge(ord, left_on='c_custkey', right_on='o_custkey')
    n1_n2_cu_su_ord_join = n1_n2_cu_su_ord_join[['n_name_1', 'n_name_2', 's_suppkey', 'o_orderkey']]

    li_filt = li[(li['l_shipdate'] >= '1995-01-01') & (li['l_shipdate'] <= '1996-12-31')]
    n1_n2_cu_su_ord_li_join = n1_n2_cu_su_ord_join.merge(li_filt,
                                                         left_on=['s_suppkey', 'o_orderkey'],
                                                         right_on=['l_suppkey', 'l_orderkey'])
    n1_n2_cu_su_ord_li_join = n1_n2_cu_su_ord_li_join[
        ['n_name_1', 'n_name_2',
         'l_extendedprice', 'l_discount', 'l_shipdate']
    ]

    n1_n2_cu_su_ord_li_join['supp_nation'] = n1_n2_cu_su_ord_li_join['n1_name']
    n1_n2_cu_su_ord_li_join['cust_nation'] = n1_n2_cu_su_ord_li_join['n2_name']
    n1_n2_cu_su_ord_li_join['l_year'] = pd.DatetimeIndex(n1_n2_cu_su_ord_li_join['l_shipdate']).year
    n1_n2_cu_su_ord_li_join['volume'] = n1_n2_cu_su_ord_li_join['l_extendedprice'] * (
            1 - n1_n2_cu_su_ord_li_join['l_discount'])

    shipping = n1_n2_cu_su_ord_li_join[['supp_nation', 'cust_nation', 'l_year', 'volume']]

    result = shipping.groupby(['supp_nation', 'cust_nation', 'l_year'], as_index=False) \
        .agg(revenue=('volume', 'sum'))

    return result


def q8(pa, su, li, ord, cu, n1, n2, re):
    re_filt = re[(re['r_name'] == 'MIDDLE EAST')]

    n1_re_join = n1.merge(re_filt, left_on='n_regionkey_1', right_on='r_regionkey')
    n1_re_join = n1_re_join[['n1_nationkey']]

    pa_li_join = pa.merge(li, left_on='p_partkey', right_on='l_partkey')
    pa_li_join = pa_li_join[['l_orderkey']]

    pa_li_ord_join = pa_li_join.merge(ord, left_on='l_orderkey', right_on='o_orderkey')
    pa_li_ord_join = pa_li_ord_join[['o_custkey']]

    pa_li_ord_cu_join = pa_li_ord_join.merge(cu, left_on='o_custkey', right_on='c_custkey')
    pa_li_ord_cu_join = pa_li_ord_cu_join[['c_nationkey']]

    n1_re_pa_li_ord_cu_join = n1_re_join.merge(pa_li_ord_cu_join,
                                               left_on='n1_nationkey', right_on='c_nationkey')
    n1_re_pa_li_ord_cu_join = n1_re_pa_li_ord_cu_join[['l_suppkey']]

    n1_re_pa_li_ord_cu_su_join = n1_re_pa_li_ord_cu_join.merge(su, left_on='l_suppkey', right_on='s_suppkey')
    n1_re_pa_li_ord_cu_su_n2_join = n1_re_pa_li_ord_cu_su_join.merge(
        n2, left_on='s_nationkey', right_on='n2_nationkey')

    n1_re_pa_li_ord_cu_su_n2_join = n1_re_pa_li_ord_cu_su_n2_join[
        ['o_orderdate', 'l_extendedprice', 'l_discount', 'n_name_2']]

    n1_re_pa_li_ord_cu_su_n2_join['o_year'] = pd.DatetimeIndex(n1_re_pa_li_ord_cu_su_n2_join['o_orderdate']).year
    n1_re_pa_li_ord_cu_su_n2_join['volume'] = n1_re_pa_li_ord_cu_su_n2_join['l_extendedprice'] * (
            1 - n1_re_pa_li_ord_cu_su_n2_join['l_discount']
    )
    n1_re_pa_li_ord_cu_su_n2_join['nation'] = n1_re_pa_li_ord_cu_su_n2_join['n2_name']

    all_nations = n1_re_pa_li_ord_cu_su_n2_join[['o_year', 'volume', 'nation']]

    all_nations['val'] = all_nations.apply(lambda x: x['volume'] if x['nation'] == 'BRAZIL' else 0, axis=1)

    all_nations_agg = all_nations.groupby(['o_year'], as_index=False) \
        .agg(val_sum=('val', 'sum'),
             vol_sum=('volume', 'sum'))

    all_nations_agg['mkt_share'] = all_nations_agg['val_sum'] / all_nations_agg['vol_sum']

    result = all_nations_agg[['o_year', 'mkt_share']]

    return result


def q9(pa, su, li, ps, ord, na):
    pa_filt = pa[pa['p_name'].str.contains('green')]

    pa_ps_join = pa_filt.merge(ps, left_on='p_partkey', right_on='ps_partkey')
    pa_ps_join = pa_ps_join[['s_suppkey']]

    na_su_join = na.merge(su, left_on='n_nationkey', right_on='s_nationkey')
    na_su_join = na_su_join[['s_suppkey', 'n_name']]

    na_su_pa_ps_join = na_su_join.merge(pa_ps_join, left_on='s_suppkey', right_on='ps_suppkey')
    na_su_pa_ps_join = na_su_pa_ps_join[['ps_suppkey', 'n_name', 'ps_supplycost']]

    na_su_pa_ps_li_join = na_su_pa_ps_join.merge(
        li, left_on='ps_suppkey', right_on='l_suppkey', how='inner')
    na_su_pa_ps_li_join = na_su_pa_ps_li_join[
        ['l_orderkey', 'n_name', 'ps_supplycost', 'l_extendedprice', 'l_discount', 'l_quantity']
    ]

    na_su_pa_ps_li_ord_join = na_su_pa_ps_li_join.merge(ord, left_on='l_orderkey', right_on='o_orderkey')
    na_su_pa_ps_li_ord_join = na_su_pa_ps_li_ord_join[
        ['n_name', 'ps_supplycost', 'l_extendedprice', 'l_discount', 'l_quantity', 'o_orderdate']
    ]

    na_su_pa_ps_li_ord_join['nation'] = na_su_pa_ps_li_ord_join['n_name']
    na_su_pa_ps_li_ord_join['o_year'] = pd.DatetimeIndex(r['o_orderdate']).year
    na_su_pa_ps_li_ord_join['amount'] = na_su_pa_ps_li_ord_join['l_extendedprice'] * (
            1 - na_su_pa_ps_li_ord_join['l_discount']
    ) - na_su_pa_ps_li_ord_join['ps_supplycost'] * na_su_pa_ps_li_ord_join['l_quantity']

    profit = na_su_pa_ps_li_ord_join[['nation', 'o_year', 'amount']]

    result = profit.groupby(['nation', 'o_year'], as_index=False) \
        .agg(sum_profit=('amount', 'sum'))

    return result


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


def q11(ps, su, na):
    na_filt = na[(na['n_name'] == 'GERMANY')]

    na_su_join = na_filt.merge(su, left_on='n_nationkey', right_on='s_nationkey')

    na_su_ps_join = na_su_join.merge(ps, left_on='s_suppkey', right_on='ps_suppkey')

    agg_val = (na_su_ps_join['ps_supplycost'] * na_su_ps_join['ps_availqty'] * 0.0001).sum()

    # GOURPBY HAVING
    na_su_ps_join_filt = na_su_ps_join.groupby(['ps_partkey']).filter(
        lambda x: (x['ps_supplycost'] * x['ps_availqty']).sum() > agg_val
    )

    na_su_ps_join_filt['value'] = na_su_ps_join_filt['ps_supplycost'] * na_su_ps_join_filt['ps_availqty']

    # SELECT GROUPBY AGGREGATION
    result = na_su_ps_join_filt.groupby(['ps_partkey'], as_index=False) \
        .agg(value=('value', 'sum'))

    return result


def q12(li, ord):
    li_filt = li[(li['l_shipmode'].isin((var1, var2)))
                 & (li['l_commitdate'] < li['l_receiptdate'])
                 & (li['l_shipdate'] < li['l_commitdate'])
                 & (li['l_receiptdate'] >= '1995-01-01') & (li['l_receiptdate'] < '1996-01-01')]

    li_ord_join = ord.merge(li_filt, left_on='o_orderkey', right_on='l_orderkey')

    li_ord_join['high_line_priority'] = li_ord_join.apply(
        lambda x:
        1
        if (li_ord_join['o_orderpriority'] == '1-URGENT') | (li_ord_join['o_orderpriority'] == '2-HIGH')
        else 0,
        axis=1)

    li_ord_join['low_line_priority'] = li_ord_join.apply(
        lambda x:
        1
        if (li_ord_join['o_orderpriority'] != '1-URGENT') | (li_ord_join['o_orderpriority'] != '2-HIGH')
        else 0,
        axis=1)

    result = li_ord_join.groupby(['l_shipmode'], as_index=False) \
        .agg(high_line_count=('high_line_priority', 'sum'),
             low_line_count=('low_line_priority', 'sum'))

    return result


def q13(ord, cu):
    ord_filt = ord[~((ord['o_comment'].str.find('special') != -1)
                     & (ord['o_comment'].str.find('requests') != -1)
                     & (ord['o_comment'].str.find('special') < ord['o_comment'].str.find('requests')))]

    cu_ord_join = cu.merge(ord_filt, how='left', left_on='c_custkey', right_on='o_custkey')

    c_orders = cu_ord_join.groupby(['c_custkey'], as_index=False) \
        .agg(c_count=('o_orderkey', 'count'))

    result = c_orders.groupby(['c_count'], as_index=False) \
        .agg(custdist=('c_custkey', 'count'))

    return result


def q14(li, pa):
    li_filt = li[(li.l_shipdate >= "1995-09-01") & (li.l_shipdate < "1995-10-01")]
    pa_proj = pa[["p_partkey", "p_type"]]
    li_pa_join = pd.merge(pa_proj, li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")
    li_pa_join["A"] = li_pa_join.apply(
        lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) if x["p_type"].startswith("PROMO") else 0, axis=1)
    li_pa_join["B"] = li_pa_join.l_extendedprice * (1.0 - li_pa_join.l_discount)

    result = li_pa_join.A.sum() / li_pa_join.B.sum() * 100.0

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


def q16(ps, pa, su):
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
        )
        ]
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


def q17(li, pa):
    li_agg = li.groupby(['l_partkey'], as_index=False) \
        .agg(val=('l_quantity', 'mean'))

    li_agg['avg_quantity'] = 0.2 * li_agg['val']
    li_agg['agg_partkey'] = li_agg['l_partkey']

    li_agg = li_agg[['avg_quantity', 'agg_partkey']]

    pa_filt = pa[(pa['p_brand'] == 'Brand#23') & (pa['p_container'] == 'MED BOX')]

    pa_li_join = pa_filt.merge(li, left_on='p_partkey', right_on='l_partkey')

    pa_li_join = pa_li_join.merge(li_agg, left_on='l_partkey', right_on='agg_partkey')
    pa_li_join = pa_li_join[pa_li_join['l_quantity'] < pa_li_join['avg_quantity']]

    pa_li_join['val'] = pa_li_join['l_extendedprice'].sum()

    pa_li_join['avg_yearly'] = pa_li_join['val'] / 7.0

    result = pa_li_join[['avg_yearly']].drop_duplicates()

    return result


def q18(cu, ord, li):
    li_aggr = li \
        .groupby(["l_orderkey"]) \
        .agg(sum_quantity=("l_quantity", "sum"))

    li_filt = li_aggr[li_aggr.sum_quantity > 300].reset_index()
    li_proj = li_filt[["l_orderkey"]]

    ord_filt = ord[ord.o_orderkey.isin(li_proj["l_orderkey"])]

    cu_proj = cu[["c_custkey", "c_name"]]
    ord_cu_join = pd.merge(cu_proj, ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")
    ord_cu_join = ord_cu_join[["o_orderkey", "c_name", "c_custkey", "o_orderdate", "o_totalprice"]]

    li_ord_join = pd.merge(ord_cu_join, li, left_on="o_orderkey", right_on="l_orderkey", how="inner")

    result = li_ord_join \
        .groupby(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"]) \
        .agg(sum_quantity=("l_quantity", "sum"))

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


def q20(li, ps, pa):
    li_filt = li[(li['l_shipdate'] >= '1994-01-01') & (li['l_shipdate'] < '1995-01-01')]

    li_agg = li_filt.groupby(['l_partkey', 'l_suppkey'], as_index=False) \
        .agg(val=('l_quantity', 'sum'))

    li_agg['agg_partkey'] = li_agg['l_partkey']
    li_agg['agg_suppkey'] = li_agg['l_suppkey']
    li_agg['agg_quantity'] = li_agg['val'] * 0.5

    ps_li_join = ps.merge(li_agg,
                          left_on=['ps_partkey', 'ps_suppkey'],
                          right_on=['agg_partkey', 'agg_suppkey'])
    ps_li_filt = ps_li_join[ps_li_join['ps_availqty'] > ps_li_join['agg_quantity']]

    pa_filt = pa[pa['p_name'].str.startswith('forest')]

    ps_li_filt = ps_li_filt[(ps_li_filt['ps_partkey'].isin(pa_filt['p_partkey']))]

    na_filt = na[(na['n_name'] == 'CANADA')]

    na_su_join = na_filt.merge(su, left_on='n_nationkey', right_on='s_nationkey')

    na_su_filt = na_su_join[(na_su_join['s_suppkey'].isin(ps_li_filt['ps_suppkey']))]

    result = na_su_filt[['s_name', 's_address']]

    return result


if __name__ == '__main__':
    ord = DataFrame()
    cu = DataFrame()
    na = DataFrame()
    li = DataFrame()
    pa = DataFrame()
    su = DataFrame()
    ps = DataFrame()
    re = DataFrame()

    # q1(li)
    # q3(cu, ord, li)
    # q4(li, ord)
    q5(cu, ord, li, su, na, re)
    # q6(li)
    # q10(ord, cu, na, li)
    # q14(li, pa)
    # q15(li, su)
    # q16(ps, pa, su)
    # q18(cu, ord, li)
    # q19(pa, li)
