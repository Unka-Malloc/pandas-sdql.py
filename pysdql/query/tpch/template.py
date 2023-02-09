tpch_vars = {1: ("1998-09-02",),
             2: (15, 'BRASS', 'EUROPE'),
             3: ("BUILDING", "1995-03-15"),
             4: ("1993-07-01", "1993-10-01"),
             5: ("ASIA", "1994-01-01", "1996-12-31"),
             6: ('1994-01-01', '1995-01-01', 0.05, 0.07, 24),
             7: ('FRANCE', 'GERMANY'),
             8: ('BRAZIL', 'AMERICA', 'ECONOMY ANODIZED STEEL'),
             9: ('green'),
             10: ("1993-10-01", "1994-01-01"),
             11: ('GERMANY',),
             12: ('MAIL', 'SHIP', '1994-01-01', '1995-01-01'),
             13: ('special', 'requests'),
             14: ("1995-09-01", "1995-10-01"),
             15: ("1996-01-01", "1996-04-01", 797313.3838),
             16: ("Brand#45", "MEDIUM POLISHED", (49, 14, 23, 45, 19, 3, 36, 9)),
             17: ('Brand#11', 'WRAP CASE'),
             18: (300,),
             19: ("Brand#12", "Brand#23", "Brand#34", (1, 11), (10, 20), (20, 30)),
             20: ('forest', '1994-01-01', '1995-01-01', 'CANADA'),
             21: ("SAUDI ARABIA",),
             22: ('13', '31', '23', '29', '30', '18', '17')
             }


def tpch_q1(lineitem):
    li_filt = lineitem[(lineitem['l_shipdate'] <= tpch_vars[1][0])]
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


def tpch_q2(part, supplier, partsupp, nation, region):
    var1 = tpch_vars[2][0]
    var2 = tpch_vars[2][1]
    var3 = tpch_vars[2][2]

    ps1 = partsupp.copy()

    re_filt = region[region['r_name'] == var3]

    re_na_join = re_filt.merge(nation, left_on='r_regionkey', right_on='n_regionkey')
    re_na_join = re_na_join[['n_nationkey', 'n_name']]

    na_su_join = re_na_join.merge(supplier, left_on='n_nationkey', right_on='s_nationkey')
    na_su_join = na_su_join[['s_suppkey', 's_acctbal', 's_name', 'n_name', 's_address', 's_phone', 's_comment']]

    pa_filt = part[(part['p_type'].str.endswith(var2)) & (part['p_size'] == var1)]
    pa_filt = pa_filt[['p_partkey', 'p_mfgr']]

    # Minimum aggregation

    su_ps1_join = na_su_join.merge(ps1, left_on='s_suppkey', right_on='ps_suppkey')

    min_agg = su_ps1_join.groupby(['ps_partkey'], as_index=False) \
        .agg({'ps_supplycost': 'sum'})

    min_agg.rename({'ps_supplycost': 'min_supplycost'}, axis=1, inplace=True)

    pa_ps_join = pa_filt.merge(partsupp, left_on='p_partkey', right_on='ps_partkey')

    pa_ps_min = min_agg.merge(pa_ps_join, left_on='ps_partkey', right_on='ps_partkey')

    all_join = na_su_join.merge(pa_ps_min, left_on='s_suppkey', right_on='ps_suppkey')
    all_join = all_join[all_join['ps_supplycost'] == all_join['min_supplycost']]

    result = all_join[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]

    return result


def tpch_q3(lineitem, customer, orders):
    var1 = tpch_vars[3][0]
    var2 = tpch_vars[3][1]

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
    var1 = tpch_vars[4][0]
    var2 = tpch_vars[4][1]  # var1 + interval '3' month

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
    var1 = tpch_vars[5][0]
    var2 = tpch_vars[5][1]
    var3 = tpch_vars[5][2]

    re_filt = region[region['r_name'] == var1]

    re_na_join = re_filt.merge(right=nation, left_on='r_regionkey', right_on='n_regionkey')

    na_cu_join = re_na_join.merge(right=customer, left_on='n_nationkey', right_on='c_nationkey')

    ord_filt = orders[(orders['o_orderdate'] >= var2) & (orders['o_orderdate'] < var3)]
    cu_ord_join = na_cu_join.merge(right=ord_filt, left_on='c_custkey', right_on='o_custkey')

    ord_li_join = cu_ord_join.merge(right=lineitem, left_on='o_orderkey', right_on='l_orderkey')

    su_ord_li_join = supplier.merge(right=ord_li_join,
                                    left_on=['s_suppkey', 's_nationkey'],
                                    right_on=['l_suppkey', 'c_nationkey'])

    su_ord_li_join['revenue'] = su_ord_li_join['l_extendedprice'] * (1.0 - su_ord_li_join['l_discount'])

    result = su_ord_li_join.groupby(['n_name'], as_index=False).agg(revenue=('revenue', 'sum'))

    return result


def tpch_q6(lineitem):
    var1 = tpch_vars[6][0]
    var2 = tpch_vars[6][1]
    var3 = tpch_vars[6][2]
    var4 = tpch_vars[6][3]
    var5 = tpch_vars[6][4]

    li_filt = lineitem[
        (lineitem.l_shipdate >= var1) &
        (lineitem.l_shipdate < var2) &
        (lineitem.l_discount >= var3) &
        (lineitem.l_discount <= var4) &
        (lineitem.l_quantity < var5)
        ]

    li_filt['revenue'] = li_filt.l_extendedprice * li_filt.l_discount

    result = li_filt.agg({'revenue': 'sum'})

    return result


def tpch_q7(supplier, lineitem, orders, customer, nation):
    # 1M
    var1 = tpch_vars[7][0]
    var2 = tpch_vars[7][1]

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
    all_join['volume'] = all_join['l_extendedprice'] * (1.0 - all_join['l_discount'])

    shipping = all_join[['supp_nation', 'cust_nation', 'l_year', 'volume']]

    result = shipping.groupby(['supp_nation', 'cust_nation', 'l_year'], as_index=False) \
        .agg(revenue=('volume', 'sum'))

    return result


def tpch_q8(part, supplier, lineitem, orders, customer, nation, region):
    # 1G
    # var1 = 'BRAZIL'
    var2 = tpch_vars[8][1]
    var3 = tpch_vars[8][2]

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
    var1 = tpch_vars[9][0]

    '''
    由于n_name作为record的value会被concatenate, 
    所以我们直接取nation_part中的n_name,
    以此绕过SDQL.py的缺陷
    '''

    na_su_join = nation.merge(supplier, left_on='n_nationkey', right_on='s_nationkey')

    pa_filt = part[part['p_name'].str.contains(var1)]

    pa_ps_join = pa_filt.merge(partsupp, left_on='p_partkey', right_on='ps_partkey')

    su_ps_join = na_su_join.merge(pa_ps_join, left_on='s_suppkey', right_on='ps_suppkey')

    ord_li_join = orders.merge(lineitem, left_on='o_orderkey', right_on='l_orderkey')

    all_join = su_ps_join.merge(ord_li_join,
                                left_on=['ps_partkey', 'ps_suppkey'],
                                right_on=['l_partkey', 'l_suppkey'])

    all_join['nation'] = all_join['n_name']
    all_join['o_year'] = all_join['o_orderdate'].dt.year
    all_join['amount'] = (all_join['l_extendedprice'] * (1.0 - all_join['l_discount'])) - (
            all_join['ps_supplycost'] * all_join['l_quantity'])

    profit = all_join[['nation', 'o_year', 'amount']]

    result = profit.groupby(['nation', 'o_year'], as_index=False) \
        .agg(sum_profit=('amount', 'sum'))

    return result


def tpch_q10(customer, orders, lineitem, nation):
    var1 = tpch_vars[10][0]
    var2 = tpch_vars[10][1]

    ord_filt = orders[(orders['o_orderdate'] >= var1) & (orders['o_orderdate'] < var2)]

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
    var1 = tpch_vars[11][0]

    # 1M
    # var1 = 'PERU'
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
    var1 = (tpch_vars[12][0], tpch_vars[12][1])
    var2 = tpch_vars[12][2]
    var3 = tpch_vars[12][3]

    li_filt = lineitem[(lineitem['l_shipmode'].isin(var1))
                       & (lineitem['l_commitdate'] < lineitem['l_receiptdate'])
                       & (lineitem['l_shipdate'] < lineitem['l_commitdate'])
                       & (lineitem['l_receiptdate'] >= var2) & (lineitem['l_receiptdate'] < var3)]

    li_ord_join = li_filt.merge(orders, left_on='l_orderkey', right_on='o_orderkey')

    li_ord_join['high_line_priority'] = li_ord_join.apply(
        lambda x: 1 if ((x['o_orderpriority'] == '1-URGENT') | (x['o_orderpriority'] == '2-HIGH')) else 0,
        axis=1)

    li_ord_join['low_line_priority'] = li_ord_join.apply(
        lambda x: 1 if ((x['o_orderpriority'] != '1-URGENT') & (x['o_orderpriority'] != '2-HIGH')) else 0,
        axis=1)

    result = li_ord_join.groupby(['l_shipmode'], as_index=False) \
        .agg(high_line_count=('high_line_priority', 'sum'),
             low_line_count=('low_line_priority', 'sum'))

    return result


def tpch_q13(customer, orders):
    var1 = tpch_vars[13][0]
    var2 = tpch_vars[13][1]

    ord_filt = orders[~((orders['o_comment'].str.find(var1) != -1)
                        & (orders['o_comment'].str.find(var2) > (orders['o_comment'].str.find(var1) + 6)))]

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
    var1 = tpch_vars[14][0]
    var2 = tpch_vars[14][1]  # var1 + interval '1' month

    li_filt = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)]

    li_pa_join = part.merge(li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")

    li_pa_join["A"] = li_pa_join.apply(
        lambda x: x["l_extendedprice"] * (1.0 - x["l_discount"]) if x["p_type"].startswith("PROMO") else 0.0,
        axis=1)
    li_pa_join["B"] = li_pa_join['l_extendedprice'] * (1.0 - li_pa_join['l_discount'])

    result = li_pa_join['A'].sum() * 100.0 / li_pa_join['B'].sum()

    return result


def tpch_q15(lineitem, supplier):
    var1 = tpch_vars[15][0]
    var2 = tpch_vars[15][1]
    var3 = tpch_vars[15][2]

    li_filt = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)]
    li_filt["revenue"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount'])

    li_aggr = li_filt \
        .groupby(["l_suppkey"]) \
        .agg(total_revenue=("revenue", "sum"))

    # maximum:
    # 1G -> 1772627.2087
    # 100M -> 1614410.2928
    # 10M -> 1161099.4636
    # 1M -> 797313.3838

    li_aggr = li_aggr[li_aggr['total_revenue'] == var3]

    su_proj = supplier[["s_suppkey", "s_name", "s_address", "s_phone"]]
    li_su_join = su_proj.merge(li_aggr, left_on="s_suppkey", right_on="l_suppkey", how="inner")

    result = li_su_join[["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]]

    return result


def tpch_q16(partsupp, part, supplier):
    var1 = tpch_vars[16][0]
    var2 = tpch_vars[16][1]
    var3 = tpch_vars[16][2]

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
    var1 = tpch_vars[17][0]
    var2 = tpch_vars[17][1]

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
    var1 = tpch_vars[18][0]

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
    var1 = tpch_vars[19][0]
    var2 = tpch_vars[19][1]
    var3 = tpch_vars[19][2]
    var4 = tpch_vars[19][3][0]
    var5 = tpch_vars[19][3][1]
    var6 = tpch_vars[19][4][0]
    var7 = tpch_vars[19][4][1]
    var8 = tpch_vars[19][5][0]
    var9 = tpch_vars[19][5][1]

    pa_filt = part[
        ((part.p_brand == var1)
         & (part.p_container.isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"]))
         & (part.p_size >= 1) & (part.p_size <= 5)) |
        ((part.p_brand == var2)
         & (part.p_container.isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"]))
         & (part.p_size >= 1) & (part.p_size <= 10)) |
        ((part.p_brand == var3)
         & (part.p_container.isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"]))
         & (part.p_size >= 1) & (part.p_size <= 15))
        ]

    pa_proj = pa_filt[["p_partkey", "p_brand"]]

    li_filt = lineitem[(((lineitem.l_shipmode == "AIR") | (lineitem.l_shipmode == "AIR REG"))
                        & (lineitem.l_shipinstruct == "DELIVER IN PERSON"))]

    li_pa_join = pa_proj.merge(li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")
    li_pa_join_filt = li_pa_join[
        (
                ((li_pa_join.p_brand == var1)
                 & ((li_pa_join.l_quantity >= var4) & (li_pa_join.l_quantity <= var5)))
                | ((li_pa_join.p_brand == var2)
                   & ((li_pa_join.l_quantity >= var6) & (li_pa_join.l_quantity <= var7)))
                | ((li_pa_join.p_brand == var3)
                   & ((li_pa_join.l_quantity >= var8) & (li_pa_join.l_quantity <= var9)))
        )
    ]

    li_pa_join_filt["revenue"] = li_pa_join_filt['l_extendedprice'] * (1.0 - li_pa_join_filt['l_discount'])

    result = li_pa_join_filt.agg({'revenue': 'sum'})

    return result


def tpch_q20(supplier, nation, partsupp, part, lineitem):
    # 1G
    var1 = tpch_vars[20][0]
    var2 = tpch_vars[20][1]
    var3 = tpch_vars[20][2]  # var1 + interval '1' year
    var4 = tpch_vars[20][3]

    # 1M
    # var1 = 'orange'
    # var2 = '1995-01-01'
    # var3 = '1996-01-01'  # var2 + 1 year
    # var4 = 'UNITED STATES'

    li_filt = lineitem[(lineitem['l_shipdate'] >= var2) & (lineitem['l_shipdate'] < var3)]

    pa_filt = part[part['p_name'].str.startswith(var1)]

    li_filt = li_filt[(li_filt['l_partkey'].isin(pa_filt['p_partkey']))]

    li_filt = li_filt[(li_filt['l_suppkey'].isin(supplier['s_suppkey']))]

    agg_lineitem = li_filt.groupby(['l_partkey', 'l_suppkey'], as_index=False) \
        .agg(sum_quantity=('l_quantity', 'sum'))

    li_ps_join = agg_lineitem.merge(partsupp,
                                    left_on=['l_partkey', 'l_suppkey'],
                                    right_on=['ps_partkey', 'ps_suppkey'])

    li_ps_filt = li_ps_join[li_ps_join['ps_availqty'] > li_ps_join['sum_quantity'] * 0.5]

    na_filt = nation[(nation['n_name'] == var4)]

    su_filt = supplier[(supplier['s_suppkey'].isin(li_ps_filt['l_suppkey']))]

    na_su_join = na_filt.merge(su_filt, left_on='n_nationkey', right_on='s_nationkey')

    result = na_su_join[['s_name', 's_address']]

    return result


def tpch_q21(suppier, lineitem, orders, nation):
    var1 = tpch_vars[21][0]

    l2 = lineitem.copy()
    l3 = lineitem.copy()

    na_filt = nation[nation['n_name'] == var1]
    na_su_join = na_filt.merge(suppier, left_on='n_nationkey', right_on='s_nationkey')

    ord_filt = orders[orders['o_orderstatus'] == "F"]

    l2_agg = l2.groupby(['l_orderkey'], as_index=False).agg(l2_size=('l_suppkey', 'count'))
    l2_filt = l2_agg[['l_orderkey', 'l2_size']]

    l3_filt = l3[(l3['l_receiptdate'] > l3['l_commitdate'])]
    l3_agg = l3_filt.groupby(['l_orderkey'], as_index=False).agg(l3_size=('l_suppkey', 'count'))
    l3_filt = l3_agg[['l_orderkey', 'l3_size']]

    l1 = lineitem[(lineitem['l_receiptdate'] > lineitem['l_commitdate'])]

    l1_l2_join = l2_filt.merge(l1, left_on='l_orderkey', right_on='l_orderkey')

    l1_l3_join = l3_filt.merge(l1_l2_join, left_on='l_orderkey', right_on='l_orderkey')

    su_li_join = na_su_join.merge(l1_l3_join, left_on='s_suppkey', right_on='l_suppkey')

    ord_li_join = ord_filt.merge(su_li_join, left_on='o_orderkey', right_on='l_orderkey')
    ord_li_join = ord_li_join[(ord_li_join['l2_size'] > 1) & (ord_li_join['l3_size'] == 1)]

    result = ord_li_join.groupby(['s_name'], as_index=False) \
        .agg(numwait=('s_name', 'count'))

    return result


def tpch_q22(customer, orders):
    var1 = tpch_vars[22]

    cu1 = customer.copy()

    cu1_filt = cu1[(cu1['c_acctbal'] > 0.00)
                   & (cu1['c_phone'].str.startswith(var1[0])
                      | cu1['c_phone'].str.startswith(var1[1])
                      | cu1['c_phone'].str.startswith(var1[2])
                      | cu1['c_phone'].str.startswith(var1[3])
                      | cu1['c_phone'].str.startswith(var1[4])
                      | cu1['c_phone'].str.startswith(var1[5])
                      | cu1['c_phone'].str.startswith(var1[6]))]

    cu1_agg = cu1_filt.agg(sum_acctbal=('c_acctbal', 'sum'),
                           count_acctbal=('c_acctbal', 'count')).squeeze()

    cu1_avg = cu1_agg['sum_acctbal'] / cu1_agg['count_acctbal']

    cu_filt = customer[(customer['c_acctbal'] > cu1_avg)
                       & (customer['c_phone'].str.startswith(var1[0])
                          | customer['c_phone'].str.startswith(var1[1])
                          | customer['c_phone'].str.startswith(var1[2])
                          | customer['c_phone'].str.startswith(var1[3])
                          | customer['c_phone'].str.startswith(var1[4])
                          | customer['c_phone'].str.startswith(var1[5])
                          | customer['c_phone'].str.startswith(var1[6]))]

    custsale = cu_filt[~cu_filt['c_custkey'].isin(orders['o_custkey'])]
    custsale['cntrycode'] = customer['c_phone'].str.slice(0, 2)

    result = custsale.groupby(['cntrycode'], as_index=False).agg(numcust=('c_acctbal', 'count'),
                                                                 totacctbal=('c_acctbal', 'sum'))

    return result
