import pandas as pd


def tpch_q1(lineitem):
    li_filt = lineitem[(lineitem['l_shipdate'] <= "1998-09-02")]
    li_filt["disc_price"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount'])
    li_filt["charge"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount']) * (1.0 + li_filt['l_tax'])

    result = li_filt \
        .groupby(["l_returnflag", "l_linestatus"], as_index=False) \
        .agg(sum_qty=("l_quantity", "sum"),
             sum_base_price=("l_extendedprice", "sum"),
             sum_disc_price=("disc_price", "sum"),
             sum_charge=("charge", "sum"),
             count_order=("l_quantity", "count"))

    return result


def tpch_q3(lineitem, customer, orders):
    var1 = "BUILDING"
    var2 = "1995-03-15"

    cu_filt = customer[customer['c_mktsegment'] == var1]

    ord_filt = orders[orders['o_orderdate'] < var2]

    cu_ord_join = cu_filt.merge(ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")

    li_filt = lineitem[lineitem['l_shipdate'] > var2]
    li_ord_join = cu_ord_join.merge(li_filt, left_on="o_orderkey", right_on="l_orderkey", how="inner")
    li_ord_join["revenue"] = li_ord_join['l_extendedprice'] * (1.0 - li_ord_join['l_discount'])

    result = li_ord_join \
        .groupby(["l_orderkey", "o_orderdate", "o_shippriority"], as_index=False) \
        .agg({'revenue': 'sum'})

    return result


def tpch_q4(orders, lineitem):
    var1 = "1993-07-01"
    var2 = "1993-10-01"  # var1 + interval '3' month

    li_filt = lineitem[lineitem.l_commitdate < lineitem.l_receiptdate]
    li_proj = li_filt[["l_orderkey"]]

    ord_filt = orders[(orders.o_orderdate >= var1)
                      & (orders.o_orderdate < var2)
                      & orders.o_orderkey.isin(li_proj["l_orderkey"])]

    results = ord_filt \
        .groupby(["o_orderpriority"], as_index=False) \
        .agg(order_count=("o_orderdate", "count"))

    return results


def tpch_q5(lineitem, customer, orders, region, nation, supplier):
    # var1 = "ASIA"
    var1 = "MIDDLE EAST"

    re_filt = region[region['r_name'] == var1]

    re_na_join = re_filt.merge(right=nation, left_on='r_regionkey', right_on='n_regionkey')

    na_cu_join = re_na_join.merge(right=customer, left_on='n_nationkey', right_on='c_nationkey')

    ord_filt = orders[(orders['o_orderdate'] >= '1995-01-01') & (orders['o_orderdate'] < '1996-01-01')]
    cu_ord_join = na_cu_join.merge(right=ord_filt, left_on='c_custkey', right_on='o_custkey')

    ord_li_join = cu_ord_join.merge(right=lineitem, left_on='o_orderkey', right_on='l_orderkey')

    su_ord_li_join = supplier.merge(right=ord_li_join,
                                    left_on=['s_suppkey', 's_nationkey'],
                                    right_on=['l_suppkey', 'c_nationkey'])

    su_ord_li_join['revenue'] = su_ord_li_join['l_extendedprice'] * (1 - su_ord_li_join['l_discount'])

    result = su_ord_li_join.groupby(['n_name'], as_index=False).agg(revenue=('revenue', 'sum'))

    return result


def tpch_q6(lineitem):
    var1 = 4
    var2 = 0.06
    var3 = 24

    li_filt = lineitem[
        (lineitem.l_shipdate >= f"199{var1}-01-01") &
        (lineitem.l_shipdate < f"199{var1 + 1}-01-01") &
        (lineitem.l_discount >= var2 - 0.01) &
        (lineitem.l_discount <= var2 + 0.01) &
        (lineitem.l_quantity < var3)
        ]

    li_filt['revenue'] = li_filt.l_extendedprice * li_filt.l_discount

    result = li_filt.agg({'revenue': 'sum'})

    return result


def tpch_q7(supplier, lineitem, orders, customer, nation):
    # var1 = 'FRANCE'
    # var2 = 'GERMANY'

    # 1M
    var1 = 'PERU'
    var2 = 'MOROCCO'

    na_filt = nation[(nation['n_name'] == var1) | (nation['n_name'] == var2)]

    na_su_join = nation.merge(right=supplier,
                              left_on='n_nationkey', right_on='s_nationkey',
                              how='inner')

    na_su_join.rename({'n_name': 'n1_name'}, axis=1, inplace=True)

    na_cu_join = na_filt.merge(right=customer,
                               left_on='n_nationkey', right_on='c_nationkey',
                               how='inner')

    cu_ord_join = na_cu_join.merge(right=orders,
                                   left_on='c_custkey', right_on='o_custkey',
                                   how='inner')

    cu_ord_join.rename({'n_name': 'n2_name'}, axis=1, inplace=True)

    li_filt = lineitem[(lineitem['l_shipdate'] >= '1995-01-01') & (lineitem['l_shipdate'] <= '1996-12-31')]

    ord_li_join = cu_ord_join.merge(right=li_filt,
                                    left_on='o_orderkey', right_on='l_orderkey',
                                    how='inner')

    all_join = na_su_join.merge(right=ord_li_join,
                                left_on='s_suppkey', right_on='l_suppkey',
                                how='inner')

    all_join = all_join[((all_join['n1_name'] == var1) & (all_join['n2_name'] == var2))
                        | ((all_join['n1_name'] == var2) & (all_join['n2_name'] == var1))]

    all_join['supp_nation'] = all_join['n1_name']
    all_join['cust_nation'] = all_join['n2_name']
    all_join['l_year'] = all_join['l_shipdate'].dt.year
    all_join['volume'] = all_join['l_extendedprice'] * (1 - all_join['l_discount'])

    shipping = all_join[['supp_nation', 'cust_nation', 'l_year', 'volume']]

    result = shipping.groupby(['supp_nation', 'cust_nation', 'l_year'], as_index=False) \
        .agg(revenue=('volume', 'sum'))

    return result


def tpch_q8(part, supplier, lineitem, orders, customer, nation, region):
    # 1G
    # var1 = 'BRAZIL'
    var2 = 'AMERICA'
    var3 = 'ECONOMY ANODIZED STEEL'

    n1 = nation.copy()

    n1.rename({'n_nationkey': 'n1_nationkey',
               'n_name': 'n1_name',
               'n_regionkey': 'n1_regionkey',
               'n_comment': 'n1_comment'}, axis=1, inplace=True)

    n2 = nation.copy()

    n2.rename({'n_nationkey': 'n2_nationkey',
               'n_name': 'n2_name',
               'n_regionkey': 'n2_regionkey',
               'n_comment': 'n2_comment'}, axis=1, inplace=True)

    re_filt = region[(region['r_name'] == var2)]

    re_na_join = re_filt.merge(right=n1, left_on='r_regionkey', right_on='n1_regionkey')

    ord_filt = orders[(orders['o_orderdate'] >= '1995-01-01') & (orders['o_orderdate'] <= '1996-12-31')]

    na_cu_join = re_na_join.merge(right=customer, left_on='n1_nationkey', right_on='c_nationkey')

    cu_ord_join = na_cu_join.merge(right=ord_filt, left_on='c_custkey', right_on='o_custkey')

    ord_li_join = cu_ord_join.merge(right=lineitem, left_on='o_orderkey', right_on='l_orderkey')

    '''
    通过 left and right 的直接传递来判断一个probe side 是否 bypass
    例如 l_orderkey -> o_orderkey -> o_custkey -> c_custkey -> c_nationkey -> n_nationkey
    region nation 是一个例外
    因为他们满足: region is not joint and nation is not joint
    此时, probe 必须发生

    当这样的传递发生时, 仅可以省略中间的传递
    在 lineitem -> orders -> customers -> nation -> region
    的传递中
    orders 同时包含 
        1. 必须的 orderdate 作为下文的变量
        2. o_custkey 作为上文的索引, 在此处, 特指 对 customers 的索引 o_custkey
        通过 o_custkey, 可以直接取值 o_custkey -> c_custkey
    customer 作为唯一的 complete bypass dictionary, 也可以称之为 indexing bypass dictionary,
    它将 c_custkey 索引至 c_nationkey, 也就是说, 它的 key 和 value 必须唯一

    这也就是 indexing bypass 的 传递作用:
        通过直接使用 indexing, 我们可以直接判断两端 [orders, nation] 是否符合要求,
        如果两端符合要求, 则 作为 bypass dictionary 的 customer 必定符合要求
        这种直接的索引优化了非空判断 (non null assertion optimization)

    在这个例子中, li 按照顺序检查了 [part, orders, region_n1_joint]
    这同时意味着, [n2, supplier, customer] 全部都是 bypass partition
    '''

    pa_filt = part[part['p_type'] == var3]

    pa_li_join = pa_filt.merge(right=ord_li_join, left_on='p_partkey', right_on='l_partkey')

    su_li_join = supplier.merge(right=pa_li_join, left_on='s_suppkey', right_on='l_suppkey')

    all_join = n2.merge(right=su_li_join, left_on='n2_nationkey', right_on='s_nationkey')

    all_join['o_year'] = all_join['o_orderdate'].dt.year
    all_join['volume'] = all_join['l_extendedprice'] * (1 - all_join['l_discount'])
    all_join['nation'] = all_join['n2_name']

    all_nations = all_join[['o_year', 'volume', 'nation']]

    all_nations['volume_A'] = all_nations.apply(lambda x: x['volume'] if x['nation'] == 'BRAZIL' else 0.0, axis=1)

    all_nations_agg = all_nations.groupby(['o_year'], as_index=False) \
        .agg(A=('volume_A', 'sum'),
             B=('volume', 'sum'))

    all_nations_agg['mkt_share'] = all_nations_agg['A'] / all_nations_agg['B']

    result = all_nations_agg[['o_year', 'mkt_share']]

    return result


def tpch_q9(lineitem, orders, nation, supplier, part, partsupp):
    na_su_join = nation.merge(supplier, left_on='n_nationkey', right_on='s_nationkey')

    pa_filt = part[part['p_name'].str.contains('green')]

    pa_ps_join = pa_filt.merge(partsupp, left_on='p_partkey', right_on='ps_partkey')

    su_ps_join = na_su_join.merge(pa_ps_join, left_on='s_suppkey', right_on='ps_suppkey')

    ord_li_join = orders.merge(lineitem, left_on='o_orderkey', right_on='l_orderkey')

    all_join = su_ps_join.merge(ord_li_join, left_on='ps_suppkey', right_on='l_suppkey', how='inner')

    all_join['nation'] = all_join['n_name']
    all_join['o_year'] = all_join['o_orderdate'].dt.year
    all_join['amount'] = (all_join['l_extendedprice'] * (1.0 - all_join['l_discount'])) - (
            all_join['ps_supplycost'] * all_join['l_quantity'])

    profit = all_join[['nation', 'o_year', 'amount']]

    result = profit.groupby(['nation', 'o_year'], as_index=False) \
        .agg(sum_profit=('amount', 'sum'))

    return result


def tpch_q10(customer, orders, lineitem, nation):
    ord_filt = orders[(orders['o_orderdate'] >= "1993-10-01") & (orders['o_orderdate'] < "1994-01-01")]

    cu_proj = customer[["c_custkey", "c_name", "c_acctbal", "c_phone", "c_address", "c_comment", "c_nationkey"]]

    ord_cu_join = cu_proj.merge(ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")

    na_cu_join = nation.merge(ord_cu_join, left_on="n_nationkey", right_on="c_nationkey", how="inner")
    na_cu_join = na_cu_join[
        ["o_orderkey", "c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"]]

    li_filt = lineitem[(lineitem['l_returnflag'] == "R")]

    li_ord_join = na_cu_join.merge(li_filt, left_on="o_orderkey", right_on="l_orderkey", how="inner")

    li_ord_join["revenue"] = li_ord_join.l_extendedprice * (1.0 - li_ord_join.l_discount)

    result = li_ord_join \
        .groupby(["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"],
                 as_index=False) \
        .agg({"revenue": 'sum'})

    return result


def tpch_q11(partsupp, supplier, nation):
    # 1G
    var1 = 'GERMANY'

    # 1M
    var1 = 'PERU'
    na_filt = nation[(nation['n_name'] == var1)]

    na_su_join = na_filt.merge(supplier, left_on='n_nationkey', right_on='s_nationkey')

    all_join = na_su_join.merge(partsupp, left_on='s_suppkey', right_on='ps_suppkey')

    agg_val = (all_join['ps_supplycost'] * all_join['ps_availqty']).sum() * 0.0001

    # GOURPBY HAVING
    all_join_filt = all_join.groupby(['ps_partkey']).filter(
        lambda x: (x['ps_supplycost'] * x['ps_availqty']).sum() > agg_val
    )

    all_join_filt['value'] = all_join_filt['ps_supplycost'] * all_join_filt['ps_availqty']

    # SELECT GROUPBY AGGREGATION
    result = all_join_filt.groupby(['ps_partkey'], as_index=False) \
        .agg({'value': 'sum'})

    return result


def tpch_q12(orders, lineitem):
    var1 = ('MAIL', 'SHIP')

    li_filt = lineitem[(lineitem['l_shipmode'].isin(var1))
                       & (lineitem['l_commitdate'] < lineitem['l_receiptdate'])
                       & (lineitem['l_shipdate'] < lineitem['l_commitdate'])
                       & (lineitem['l_receiptdate'] >= '1995-01-01') & (lineitem['l_receiptdate'] < '1996-01-01')]

    li_ord_join = li_filt.merge(orders, left_on='l_orderkey', right_on='o_orderkey')

    li_ord_join['high_line_priority'] = li_ord_join.apply(
        lambda x: 1 if ((x['o_orderpriority'] == '1-URGENT') | (x['o_orderpriority'] == '2-HIGH')) else 0,
        axis=1)

    li_ord_join['low_line_priority'] = li_ord_join.apply(
        lambda x: 1 if ((x['o_orderpriority'] != '1-URGENT') | (x['o_orderpriority'] != '2-HIGH')) else 0,
        axis=1)

    result = li_ord_join.groupby(['l_shipmode'], as_index=False) \
        .agg(high_line_count=('high_line_priority', 'sum'),
             low_line_count=('low_line_priority', 'sum'))

    return result


def tpch_q13(customer, orders):
    ord_filt = orders[~((orders['o_comment'].str.find('special') != -1)
                        & (orders['o_comment'].str.find('requests') > (orders['o_comment'].str.find('special') + 6)))]

    # customer left outer join ord_filt
    # is equivalent to
    # ord_filt right outer join customer

    cu_ord_join = ord_filt.merge(customer, how='right', left_on='o_custkey', right_on='c_custkey')

    c_orders = cu_ord_join.groupby(['c_custkey'], as_index=False) \
        .agg(c_count=('o_orderkey', 'count'))

    result = c_orders.groupby(['c_count'], as_index=False) \
        .agg(custdist=('c_custkey', 'count'))

    return result


def tpch_q14(lineitem, part):
    var1 = "1995-09-01"
    var2 = "1995-10-01"  # var1 + interval '1' month

    li_filt = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)]

    li_pa_join = part.merge(li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")

    li_pa_join["A"] = li_pa_join.apply(
        lambda x: x["l_extendedprice"] * (1.0 - x["l_discount"]) if x["p_type"].startswith("PROMO") else 0.0,
        axis=1)
    li_pa_join["B"] = li_pa_join['l_extendedprice'] * (1.0 - li_pa_join['l_discount'])

    result = li_pa_join['A'].sum() / li_pa_join['B'].sum() * 100.0

    return result


def tpch_q15(lineitem, supplier):
    li_filt = lineitem[(lineitem['l_shipdate'] >= "1996-01-01") & (lineitem['l_shipdate'] < "1996-04-01")]
    li_filt["revenue"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount'])

    li_aggr = li_filt \
        .groupby(["l_suppkey"]) \
        .agg(total_revenue=("revenue", "sum"))

    # maximum:
    # 1G -> 1772627.2087
    # 100M -> 1614410.2928
    # 10M -> 1161099.4636
    # 1M -> 797313.3838

    li_aggr = li_aggr[li_aggr['total_revenue'] == 797313.3838]

    su_proj = supplier[["s_suppkey", "s_name", "s_address", "s_phone"]]
    li_su_join = su_proj.merge(li_aggr, left_on="s_suppkey", right_on="l_suppkey", how="inner")

    result = li_su_join[["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]]

    return result


def tpch_q16(partsupp, part, supplier):
    var1 = "Brand#45"
    var2 = "MEDIUM POLISHED"
    var3 = (49, 14, 23, 45, 19, 3, 36, 9)

    pa_filt = part[
        (part.p_brand != var1) &
        (part.p_type.str.startswith(var2) == False) &
        (part.p_size.isin(var3))]
    pa_proj = pa_filt[["p_partkey", "p_brand", "p_type", "p_size"]]

    su_filt = supplier[
        supplier.s_comment.str.contains("Customer")
        & ((supplier.s_comment.str.find("Customer") + 7) < supplier.s_comment.str.find("Complaints"))]
    su_proj = su_filt[["s_suppkey"]]

    ps_filt = partsupp[~partsupp.ps_suppkey.isin(su_proj["s_suppkey"])]

    ps_pa_join = pa_proj.merge(ps_filt, left_on="p_partkey", right_on="ps_partkey", how="inner")

    result = ps_pa_join \
        .groupby(["p_brand", "p_type", "p_size"], as_index=False) \
        .agg(supplier_cnt=("ps_suppkey", lambda x: x.nunique()))

    return result


def tpch_q17(lineitem, part):
    # 1G
    # var1 = 'Brand#23'
    # var2 = 'MED BOX'

    # 1M
    var1 = 'Brand#11'
    var2 = 'WRAP CASE'

    l1 = lineitem.copy()

    part_agg = l1.groupby(['l_partkey'], as_index=False) \
        .agg(sum_quant=('l_quantity', 'sum'),
             count_quant=('l_quantity', 'count'))

    pa_filt = part[(part['p_brand'] == var1) & (part['p_container'] == var2)]

    pa_li_join = pa_filt.merge(part_agg, left_on='p_partkey', right_on='l_partkey')

    pa_li_join = pa_li_join.merge(lineitem, left_on='l_partkey', right_on='l_partkey')
    pa_li_join['l_extendedprice'] = pa_li_join.apply(
        lambda x: x['l_extendedprice'] if (x['l_quantity'] < (0.2 * (x['sum_quant'] / x['count_quant']))) else 0.0,
        axis=1)

    result = pa_li_join['l_extendedprice'].sum() / 7.0

    return result


def tpch_q18(lineitem, customer, orders):
    # 1G
    # var1 = 300

    # 1M
    var1 = 200

    li_aggr = lineitem \
        .groupby(["l_orderkey"]) \
        .agg(sum_quantity=("l_quantity", "sum"))

    li_filt = li_aggr[li_aggr['sum_quantity'] > var1].reset_index()
    li_proj = li_filt[["l_orderkey"]]

    ord_filt = orders[orders['o_orderkey'].isin(li_proj["l_orderkey"])]

    cu_proj = customer[["c_custkey", "c_name"]]
    cu_ord_join = cu_proj.merge(ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")
    cu_ord_join = cu_ord_join[["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"]]

    li_ord_join = cu_ord_join.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey", how="inner")

    result = li_ord_join \
        .groupby(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"], as_index=False) \
        .agg(sum_quantity=("l_quantity", "sum"))

    return result


def tpch_q19(lineitem, part):
    pa_filt = part[
        ((part.p_brand == "Brand#12")
         & (part.p_container.isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"]))
         & (part.p_size >= 1) & (part.p_size <= 5)) |
        ((part.p_brand == "Brand#23")
         & (part.p_container.isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"]))
         & (part.p_size >= 1) & (part.p_size <= 10)) |
        ((part.p_brand == "Brand#34")
         & (part.p_container.isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"]))
         & (part.p_size >= 1) & (part.p_size <= 15))
        ]

    pa_proj = pa_filt[["p_partkey", "p_brand"]]

    li_filt = lineitem[(((lineitem.l_shipmode == "AIR") | (lineitem.l_shipmode == "AIR REG"))
                        & (lineitem.l_shipinstruct == "DELIVER IN PERSON"))]

    li_pa_join = pa_proj.merge(li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")
    li_pa_join_filt = li_pa_join[
        (
                ((li_pa_join.p_brand == "Brand#12")
                 & ((li_pa_join.l_quantity >= 1) & (li_pa_join.l_quantity <= 11)))
                | ((li_pa_join.p_brand == "Brand#23")
                   & ((li_pa_join.l_quantity >= 10) & (li_pa_join.l_quantity <= 20)))
                | ((li_pa_join.p_brand == "Brand#34")
                   & ((li_pa_join.l_quantity >= 20) & (li_pa_join.l_quantity <= 30)))
        )
    ]

    li_pa_join_filt["revenue"] = li_pa_join_filt['l_extendedprice'] * (1.0 - li_pa_join_filt['l_discount'])

    result = li_pa_join_filt.agg({'revenue': 'sum'})

    return result


def tpch_q20(supplier, nation, partsupp, part, lineitem):
    li_filt = lineitem[(lineitem['l_shipdate'] >= '1994-01-01') & (lineitem['l_shipdate'] < '1995-01-01')]

    agg_lineitem = li_filt.groupby(['l_partkey', 'l_suppkey'], as_index=False) \
        .agg(sum_quantity=('l_quantity', 'sum'))

    agg_lineitem['agg_partkey'] = agg_lineitem['l_partkey']
    agg_lineitem['agg_suppkey'] = agg_lineitem['l_suppkey']
    agg_lineitem['agg_quantity'] = agg_lineitem['sum_quantity'] * 0.5

    li_ps_join = agg_lineitem.merge(partsupp,
                                    left_on=['agg_partkey', 'agg_suppkey'],
                                    right_on=['ps_partkey', 'ps_suppkey'])

    pa_filt = part[part['p_name'].str.startswith('forest')]

    li_ps_filt = li_ps_join[(li_ps_join['ps_partkey'].isin(pa_filt['p_partkey']))]

    ps_li_filt = li_ps_filt[li_ps_filt['ps_availqty'] > li_ps_filt['agg_quantity']]

    na_filt = nation[(nation['n_name'] == 'CANADA')]

    na_su_join = na_filt.merge(supplier, left_on='n_nationkey', right_on='s_nationkey')

    na_su_filt = na_su_join[(na_su_join['s_suppkey'].isin(ps_li_filt['ps_suppkey']))]

    result = na_su_filt[['s_name', 's_address']]

    return result
