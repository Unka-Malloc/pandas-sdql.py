import pysdql as pd

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
    df_filter_1 = lineitem[(lineitem.l_shipdate<='1998-09-02') & (~lineitem.l_shipdate.isnull())]
    df_filter_1 = df_filter_1[['l_shipdate', 'l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax']]
    df_filter_1['before_1'] = ((df_filter_1.l_extendedprice) * (1 - (df_filter_1.l_discount)))
    df_filter_1['before_2'] = (((df_filter_1.l_extendedprice) * (1 - (df_filter_1.l_discount))) * (1 + (df_filter_1.l_tax)))
    df_group_1 = df_filter_1 \
        .groupby(['l_returnflag', 'l_linestatus'], sort=False) \
        .agg(
            sum_qty=("l_quantity", "sum"),
            sum_base_price=("l_extendedprice", "sum"),
            sum_disc_price=("before_1", "sum"),
            sum_charge=("before_2", "sum"),
            avg_qty=("l_quantity", "mean"),
            avg_price=("l_extendedprice", "mean"),
            avg_disc=("l_discount", "mean"),
            count_order=("l_returnflag", "count"),
        )
    df_group_1 = df_group_1[['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']]
    df_sort_1 = df_group_1.sort_values(by=['l_returnflag', 'l_linestatus'], ascending=[True, True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q2(part, supplier, partsupp, nation, region):
    df_filter_1 = partsupp[['ps_partkey', 'ps_suppkey', 'ps_supplycost']]
    df_filter_2 = supplier[['s_suppkey', 's_nationkey', 's_acctbal', 's_name', 's_address', 's_phone', 's_comment']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_3 = nation[['n_nationkey', 'n_regionkey', 'n_name']]
    df_filter_4 = region[(region.r_name == 'EUROPE') & (~region.r_name.isnull())]
    df_filter_4 = df_filter_4[['r_regionkey', 'r_name']]
    df_merge_2 = df_filter_3.merge(df_filter_4, left_on=['n_regionkey'], right_on=['r_regionkey'], how="inner",
                                   sort=False)
    df_merge_3 = df_merge_1.merge(df_merge_2, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                  sort=False)
    df_filter_5 = part[(part.p_size == 15) & (~part.p_size.isnull()) & (part.p_type.str.contains("^.*?BRASS$", regex=True))]
    df_filter_5 = df_filter_5[['p_partkey', 'p_size', 'p_type', 'p_mfgr']]
    df_merge_4 = df_merge_3.merge(df_filter_5, left_on=['ps_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_filter_6 = partsupp[['ps_partkey', 'ps_suppkey', 'ps_supplycost']]
    df_filter_7 = supplier[['s_suppkey', 's_nationkey']]
    df_merge_5 = df_filter_6.merge(df_filter_7, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_8 = nation[['n_nationkey', 'n_regionkey']]
    df_merge_6 = df_merge_5.merge(df_filter_8, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                  sort=False)
    df_filter_9 = region[(region.r_name == 'EUROPE') & (~region.r_name.isnull())]
    df_filter_9 = df_filter_9[['r_regionkey', 'r_name']]
    df_merge_7 = df_merge_6.merge(df_filter_9, left_on=['n_regionkey'], right_on=['r_regionkey'], how="inner",
                                  sort=False)
    df_group_1 = df_merge_7 \
        .groupby(['ps_partkey'], sort=False) \
        .agg(
        min_ps_supplycost=("ps_supplycost", "min"),
    )
    df_group_1['minps_supplycost'] = df_group_1.min_ps_supplycost
    df_group_1 = df_group_1[['minps_supplycost']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_8 = df_merge_4.merge(df_group_1, left_on=['p_partkey'], right_on=['ps_partkey'], how="inner", sort=False)
    df_merge_8 = df_merge_8[(df_merge_8.ps_supplycost == df_merge_8.minps_supplycost)]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['s_acctbal'] = (df_merge_8.s_acctbal)
    df_aggr_1['s_name'] = (df_merge_8.s_name)
    df_aggr_1['n_name'] = (df_merge_8.n_name)
    df_aggr_1['p_partkey'] = (df_merge_8.p_partkey)
    df_aggr_1['p_mfgr'] = (df_merge_8.p_mfgr)
    df_aggr_1['s_address'] = (df_merge_8.s_address)
    df_aggr_1['s_phone'] = (df_merge_8.s_phone)
    df_aggr_1['s_comment'] = (df_merge_8.s_comment)
    df_aggr_1 = df_aggr_1[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]
    df_sort_1 = df_aggr_1.sort_values(by=['s_acctbal', 'n_name', 's_name', 'p_partkey'],
                                      ascending=[False, True, True, True])
    df_limit_1 = df_sort_1.head(100)
    return df_limit_1


def tpch_q3(lineitem, customer, orders):
    df_filter_1 = lineitem[(lineitem.l_shipdate > '1995-03-15') & (~lineitem.l_shipdate.isnull())]
    df_filter_1 = df_filter_1[['l_orderkey', 'l_shipdate', 'l_extendedprice', 'l_discount']]
    df_filter_2 = orders[(orders.o_orderdate < '1995-03-15') & (~orders.o_orderdate.isnull())]
    df_filter_2 = df_filter_2[['o_custkey', 'o_orderkey', 'o_orderdate', 'o_shippriority']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner",
                                   sort=False)
    df_filter_3 = customer[(customer.c_mktsegment == 'BUILDING') & (~customer.c_mktsegment.isnull())]
    df_filter_3 = df_filter_3[['c_mktsegment', 'c_custkey']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['o_custkey'], right_on=['c_custkey'], how="inner", sort=False)
    df_merge_2['before_1'] = ((df_merge_2.l_extendedprice) * (1 - (df_merge_2.l_discount)))
    df_group_1 = df_merge_2 \
        .groupby(['l_orderkey', 'o_orderdate', 'o_shippriority'], sort=False) \
        .agg(
        revenue=("before_1", "sum"),
    )
    df_group_1 = df_group_1[['revenue']]
    df_group_1 = df_group_1.reset_index()
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['l_orderkey'] = (df_group_1.l_orderkey)
    df_aggr_1['revenue'] = (df_group_1.revenue)
    df_aggr_1['o_orderdate'] = (df_group_1.o_orderdate)
    df_aggr_1['o_shippriority'] = (df_group_1.o_shippriority)
    df_aggr_1 = df_aggr_1[['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority']]
    df_sort_1 = df_aggr_1.sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True])
    df_limit_1 = df_sort_1.head(10)
    return df_limit_1


def tpch_q4(orders, lineitem):
    df_filter_1 = orders[
        (orders.o_orderdate >= '1993-07-01') & (orders.o_orderdate < '1993-10-01') & (~orders.o_orderdate.isnull())]
    df_filter_1 = df_filter_1[['o_orderdate', 'o_orderkey', 'o_orderpriority']]
    df_filter_2 = lineitem[(lineitem.l_commitdate < lineitem.l_receiptdate)]
    df_filter_2 = df_filter_2[['l_orderkey', 'l_commitdate', 'l_receiptdate']]
    df_merge_1 = df_filter_1[df_filter_1.o_orderkey.isin(df_filter_2["l_orderkey"])]
    df_group_1 = df_merge_1 \
        .groupby(['o_orderpriority'], sort=False) \
        .agg(
        order_count=("o_orderpriority", "count"),
    )
    df_group_1 = df_group_1[['order_count']]
    df_sort_1 = df_group_1.sort_values(by=['o_orderpriority'], ascending=[True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q5(lineitem, customer, orders, region, nation, supplier):
    df_filter_1 = lineitem[['l_orderkey', 'l_suppkey', 'l_extendedprice', 'l_discount']]
    df_filter_2 = supplier[['s_suppkey', 's_nationkey']]
    df_filter_3 = nation[['n_nationkey', 'n_regionkey', 'n_name']]
    df_filter_4 = region[(region.r_name == 'ASIA') & (~region.r_name.isnull())]
    df_filter_4 = df_filter_4[['r_regionkey', 'r_name']]
    df_merge_1 = df_filter_3.merge(df_filter_4, left_on=['n_regionkey'], right_on=['r_regionkey'], how="inner",
                                   sort=False)
    df_merge_2 = df_filter_2.merge(df_merge_1, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_3 = df_filter_1.merge(df_merge_2, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_5 = orders[
        (orders.o_orderdate >= '1994-01-01') & (orders.o_orderdate < '1995-01-01') & (~orders.o_orderdate.isnull())]
    df_filter_5 = df_filter_5[['o_custkey', 'o_orderkey', 'o_orderdate']]
    df_merge_4 = df_merge_3.merge(df_filter_5, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_6 = customer[['c_custkey', 'c_nationkey']]
    df_merge_5 = df_merge_4.merge(df_filter_6, left_on=['n_nationkey', 's_nationkey', 'o_custkey'],
                                  right_on=['c_nationkey', 'c_nationkey', 'c_custkey'], how="inner", sort=False)
    df_merge_5['before_1'] = ((df_merge_5.l_extendedprice) * (1 - (df_merge_5.l_discount)))
    df_group_1 = df_merge_5 \
        .groupby(['n_name'], sort=False) \
        .agg(
        revenue=("before_1", "sum"),
    )
    df_group_1 = df_group_1[['revenue']]
    df_sort_1 = df_group_1.sort_values(by=['revenue'], ascending=[False])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q6(lineitem):
    df_filter_1 = lineitem[(lineitem.l_shipdate >= '1994-01-01') & (lineitem.l_shipdate < '1995-01-01') & (~lineitem.l_shipdate.isnull()) & (
                lineitem.l_discount >= 0.050) & (lineitem.l_discount <= 0.070) & (~lineitem.l_discount.isnull()) & (
                                 lineitem.l_quantity < 24) & (~lineitem.l_quantity.isnull())]
    df_filter_1 = df_filter_1[['l_shipdate', 'l_discount', 'l_quantity', 'l_extendedprice']]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['l_extendedpricel_discount'] = ((df_filter_1.l_extendedprice) * (df_filter_1.l_discount))
    df_aggr_1 = df_aggr_1[['l_extendedpricel_discount']]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['revenue'] = [(df_aggr_1.l_extendedpricel_discount).sum()]
    df_aggr_2 = df_aggr_2[['revenue']]
    df_limit_1 = df_aggr_2.head(1)
    return df_limit_1


def tpch_q7(supplier, lineitem, orders, customer, nation):
    df_filter_1 = lineitem[(lineitem.l_shipdate >= '1995-01-01') & (lineitem.l_shipdate <= '1996-12-31') & (~lineitem.l_shipdate.isnull())]
    df_filter_1 = df_filter_1[['l_suppkey', 'l_orderkey', 'l_shipdate', 'l_extendedprice', 'l_discount']]
    df_filter_2 = orders[['o_orderkey', 'o_custkey']]
    df_filter_3 = customer[['c_custkey', 'c_nationkey']]
    df_filter_4 = nation[['n_nationkey', 'n_name']]
    df_merge_1 = df_filter_3.merge(df_filter_4, left_on=['c_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_2 = df_filter_2.merge(df_merge_1, left_on=['o_custkey'], right_on=['c_custkey'], how="inner", sort=False)
    df_merge_3 = df_filter_1.merge(df_merge_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_5 = supplier[['s_suppkey', 's_nationkey']]
    df_filter_6 = nation[['n_nationkey', 'n_name']]
    df_merge_4 = df_filter_5.merge(df_filter_6, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_5 = df_merge_3.merge(df_merge_4, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_merge_5 = df_merge_5[
        (df_merge_5.n_name_x == 'FRANCE') & (df_merge_5.n_name_y == 'GERMANY') | (df_merge_5.n_name_x == 'GERMANY') & (
                    df_merge_5.n_name_y == 'FRANCE')]
    df_merge_5['n1n_name'] = df_merge_5['n_name_y']
    df_merge_5['n2n_name'] = df_merge_5['n_name_x']
    df_merge_5['supp_nation'] = df_merge_5.n1n_name
    df_merge_5['cust_nation'] = df_merge_5.n2n_name
    df_merge_5['l_year'] = df_merge_5.l_shipdate.dt.year
    df_merge_5['volume'] = ((df_merge_5.l_extendedprice) * (1 - (df_merge_5.l_discount)))
    df_group_1 = df_merge_5 \
        .groupby(['supp_nation', 'cust_nation', 'l_year'], sort=False) \
        .agg(
        revenue=("volume", "sum"),
    )
    df_group_1 = df_group_1[['revenue']]
    df_sort_1 = df_group_1.sort_values(by=['supp_nation', 'cust_nation', 'l_year'], ascending=[True, True, True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q8(part, supplier, lineitem, orders, customer, nation, region):
    df_filter_1 = lineitem[['l_partkey', 'l_suppkey', 'l_orderkey', 'l_extendedprice', 'l_discount']]
    df_filter_2 = orders[
        (orders.o_orderdate >= '1995-01-01') & (orders.o_orderdate <= '1996-12-31') & (~orders.o_orderdate.isnull())]
    df_filter_2 = df_filter_2[['o_orderkey', 'o_custkey', 'o_orderdate']]
    df_filter_3 = customer[['c_custkey', 'c_nationkey']]
    df_filter_4 = nation[['n_nationkey', 'n_regionkey']]
    df_filter_5 = region[(region.r_name == 'AMERICA') & (~region.r_name.isnull())]
    df_filter_5 = df_filter_5[['r_regionkey', 'r_name']]
    df_merge_1 = df_filter_4.merge(df_filter_5, left_on=['n_regionkey'], right_on=['r_regionkey'], how="inner",
                                   sort=False)
    df_merge_2 = df_filter_3.merge(df_merge_1, left_on=['c_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_3 = df_filter_2.merge(df_merge_2, left_on=['o_custkey'], right_on=['c_custkey'], how="inner", sort=False)
    df_merge_4 = df_filter_1.merge(df_merge_3, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_6 = supplier[['s_suppkey', 's_nationkey']]
    df_filter_7 = nation[['n_nationkey', 'n_name']]
    df_merge_5 = df_filter_6.merge(df_filter_7, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_6 = df_merge_4.merge(df_merge_5, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_8 = part[(part.p_type == 'ECONOMY ANODIZED STEEL') & (~part.p_type.isnull())]
    df_filter_8 = df_filter_8[['p_partkey', 'p_type']]
    df_merge_7 = df_merge_6.merge(df_filter_8, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_merge_7['case_a'] = df_merge_7.apply(
        lambda x: (x["l_extendedprice"] * (1 - x["l_discount"])) if (x["n_name"] == 'BRAZIL') else 0, axis=1)
    df_merge_7['o_year'] = df_merge_7.o_orderdate.dt.year
    df_merge_7['volume'] = ((df_merge_7.l_extendedprice) * (1 - (df_merge_7.l_discount)))
    df_group_1 = df_merge_7 \
        .groupby(['o_year'], sort=False) \
        .agg(
        sum_case_a=("case_a", "sum"),
        sum_volume=("volume", "sum"),
    )
    df_group_1['sumcase_a'] = df_group_1.sum_case_a
    df_group_1['suml_extendedprice1l_discount'] = df_group_1.sum_volume
    df_group_1 = df_group_1[['sumcase_a', 'suml_extendedprice1l_discount']]
    df_group_1 = df_group_1.reset_index()
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['o_year'] = (df_group_1.o_year)
    df_aggr_1['mkt_share'] = ((df_group_1.sumcase_a) / (df_group_1.suml_extendedprice1l_discount))
    df_aggr_1 = df_aggr_1[['o_year', 'mkt_share']]
    df_sort_1 = df_aggr_1.sort_values(by=['o_year'], ascending=[True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q9(lineitem, orders, nation, supplier, part, partsupp):
    df_filter_1 = lineitem[['l_suppkey', 'l_partkey', 'l_orderkey', 'l_extendedprice', 'l_discount', 'l_quantity']]
    df_filter_2 = partsupp[['ps_suppkey', 'ps_partkey', 'ps_supplycost']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_suppkey', 'l_partkey'],
                                   right_on=['ps_suppkey', 'ps_partkey'], how="inner", sort=False)
    df_filter_3 = orders[['o_orderkey', 'o_orderdate']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_4 = supplier[['s_suppkey', 's_nationkey']]
    df_filter_5 = nation[['n_nationkey', 'n_name']]
    df_merge_3 = df_filter_4.merge(df_filter_5, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_4 = df_merge_2.merge(df_merge_3, left_on=['ps_suppkey', 'l_suppkey'], right_on=['s_suppkey', 's_suppkey'],
                                  how="inner", sort=False)
    df_filter_6 = part[(part.p_name.str.contains("^.*?green.*?$", regex=True))]
    df_filter_6 = df_filter_6[['p_partkey', 'p_name']]
    df_merge_5 = df_merge_4.merge(df_filter_6, left_on=['ps_partkey', 'l_partkey'], right_on=['p_partkey', 'p_partkey'],
                                  how="inner", sort=False)
    df_merge_5['nation'] = df_merge_5.n_name
    df_merge_5['o_year'] = df_merge_5.o_orderdate.dt.year
    df_merge_5['amount'] = (((df_merge_5.l_extendedprice) * (1 - (df_merge_5.l_discount))) - (
                (df_merge_5.ps_supplycost) * (df_merge_5.l_quantity)))
    df_group_1 = df_merge_5 \
        .groupby(['nation', 'o_year'], sort=False) \
        .agg(
        sum_profit=("amount", "sum"),
    )
    df_group_1 = df_group_1[['sum_profit']]
    df_sort_1 = df_group_1.sort_values(by=['nation', 'o_year'], ascending=[True, False])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q10(customer, orders, lineitem, nation):
    df_filter_1 = lineitem[(lineitem.l_returnflag == 'R') & (~lineitem.l_returnflag.isnull())]
    df_filter_1 = df_filter_1[['l_orderkey', 'l_returnflag', 'l_extendedprice', 'l_discount']]
    df_filter_2 = orders[
        (orders.o_orderdate >= '1993-10-01') & (orders.o_orderdate < '1994-01-01') & (~orders.o_orderdate.isnull())]
    df_filter_2 = df_filter_2[['o_custkey', 'o_orderkey', 'o_orderdate']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner",
                                   sort=False)
    df_filter_3 = customer[['c_custkey', 'c_nationkey', 'c_name', 'c_acctbal', 'c_phone', 'c_address', 'c_comment']]
    df_filter_4 = nation[['n_nationkey', 'n_name']]
    df_merge_2 = df_filter_3.merge(df_filter_4, left_on=['c_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_3 = df_merge_1.merge(df_merge_2, left_on=['o_custkey'], right_on=['c_custkey'], how="inner", sort=False)
    df_merge_3['before_1'] = ((df_merge_3.l_extendedprice) * (1 - (df_merge_3.l_discount)))
    df_group_1 = df_merge_3 \
        .groupby(['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment'], sort=False) \
        .agg(
        revenue=("before_1", "sum"),
    )
    df_group_1 = df_group_1[['revenue']]
    df_group_1 = df_group_1.reset_index()
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['c_custkey'] = (df_group_1.c_custkey)
    df_aggr_1['c_name'] = (df_group_1.c_name)
    df_aggr_1['revenue'] = (df_group_1.revenue)
    df_aggr_1['c_acctbal'] = (df_group_1.c_acctbal)
    df_aggr_1['n_name'] = (df_group_1.n_name)
    df_aggr_1['c_address'] = (df_group_1.c_address)
    df_aggr_1['c_phone'] = (df_group_1.c_phone)
    df_aggr_1['c_comment'] = (df_group_1.c_comment)
    df_aggr_1 = df_aggr_1[
        ['c_custkey', 'c_name', 'revenue', 'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment']]
    df_sort_1 = df_aggr_1.sort_values(by=['revenue'], ascending=[False])
    df_limit_1 = df_sort_1.head(20)
    return df_limit_1


def tpch_q11(partsupp, supplier, nation):
    df_filter_1 = partsupp[['ps_suppkey', 'ps_partkey', 'ps_supplycost', 'ps_availqty']]
    df_filter_2 = supplier[['s_suppkey', 's_nationkey']]
    df_filter_3 = nation[(nation.n_name == 'GERMANY') & (~nation.n_name.isnull())]
    df_filter_3 = df_filter_3[['n_nationkey', 'n_name']]
    df_merge_1 = df_filter_2.merge(df_filter_3, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_2 = df_filter_1.merge(df_merge_1, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_merge_2['before_1'] = ((df_merge_2.ps_supplycost) * (df_merge_2.ps_availqty))
    df_group_1 = df_merge_2 \
        .groupby(['ps_partkey'], sort=False) \
        .agg(
        value=("before_1", "sum"),
    )
    df_group_1 = df_group_1[['value']]
    df_filter_4 = partsupp[['ps_suppkey', 'ps_supplycost', 'ps_availqty']]
    df_filter_5 = supplier[['s_suppkey', 's_nationkey']]
    df_filter_6 = nation[(nation.n_name == 'GERMANY') & (~nation.n_name.isnull())]
    df_filter_6 = df_filter_6[['n_nationkey', 'n_name']]
    df_merge_3 = df_filter_5.merge(df_filter_6, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_4 = df_filter_4.merge(df_merge_3, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['sumps_supplycostps_availqty00001'] = [
        (((df_merge_4.ps_supplycost) * (df_merge_4.ps_availqty)).sum() * 0.0001)]
    df_aggr_1 = df_aggr_1[['sumps_supplycostps_availqty00001']]
    df_limit_1 = df_aggr_1.head(1)
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_5 = df_group_1.merge(df_limit_1, how="cross", sort=False)
    df_merge_5 = df_merge_5[(df_merge_5.value > df_merge_5.sumps_supplycostps_availqty00001)]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['ps_partkey'] = (df_merge_5.ps_partkey)
    df_aggr_2['value'] = (df_merge_5.value)
    df_aggr_2 = df_aggr_2[['ps_partkey', 'value']]
    df_sort_1 = df_aggr_2.sort_values(by=['value'], ascending=[False])
    df_limit_2 = df_sort_1.head(1)
    return df_limit_2


def tpch_q12(orders, lineitem):
    df_filter_1 = lineitem[(lineitem.l_shipdate < '1995-01-01') & (~lineitem.l_shipdate.isnull()) & (lineitem.l_receiptdate >= '1994-01-01') & (
                lineitem.l_receiptdate < '1995-01-01') & (~lineitem.l_receiptdate.isnull()) & (lineitem.l_commitdate < '1995-01-01') & (
                         ~lineitem.l_commitdate.isnull()) & (lineitem.l_commitdate < lineitem.l_receiptdate) & (
                                 lineitem.l_shipdate < lineitem.l_commitdate) & lineitem.l_shipmode.isin(['MAIL', 'SHIP'])]
    df_filter_1 = df_filter_1[['l_orderkey', 'l_shipmode', 'l_commitdate', 'l_receiptdate', 'l_shipdate']]
    df_filter_2 = orders[['o_orderkey', 'o_orderpriority']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner",
                                   sort=False)
    df_merge_1['case_a'] = df_merge_1.apply(
        lambda x: (1) if (x['o_orderpriority'] == '1-URGENT') | (x['o_orderpriority'] == '2-HIGH') else 0, axis=1)
    df_merge_1['case_b'] = df_merge_1.apply(
        lambda x: (1) if (x['o_orderpriority'] != '1-URGENT') & (x['o_orderpriority'] != '2-HIGH') else 0, axis=1)
    df_group_1 = df_merge_1 \
        .groupby(['l_shipmode'], sort=False) \
        .agg(
        high_line_count=("case_a", "sum"),
        low_line_count=("case_b", "sum"),
    )
    df_group_1 = df_group_1[['high_line_count', 'low_line_count']]
    df_sort_1 = df_group_1.sort_values(by=['l_shipmode'], ascending=[True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q13(customer, orders):
    df_filter_1 = orders[(orders.o_comment.str.contains("^.*?special.*?requests.*?$", regex=True) == False)]
    df_filter_1 = df_filter_1[['o_custkey', 'o_comment', 'o_orderkey']]
    df_filter_2 = customer[['c_custkey']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['o_custkey'], right_on=['c_custkey'], how="right", sort=False)
    df_group_1 = df_merge_1 \
        .groupby(['c_custkey'], sort=False) \
        .agg(
        c_count=("o_orderkey", "count"),
    )
    df_group_1 = df_group_1[['c_count']]
    df_group_2 = df_group_1 \
        .groupby(['c_count'], sort=False) \
        .agg(
        custdist=("c_count", "count"),
    )
    df_group_2 = df_group_2[['custdist']]
    df_sort_1 = df_group_2.sort_values(by=['custdist', 'c_count'], ascending=[False, False])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q14(lineitem, part):
    df_filter_1 = lineitem[(lineitem.l_shipdate >= '1995-09-01') & (lineitem.l_shipdate < '1995-10-01') & (~lineitem.l_shipdate.isnull())]
    df_filter_1 = df_filter_1[['l_partkey', 'l_shipdate', 'l_extendedprice', 'l_discount']]
    df_filter_2 = part[['p_partkey', 'p_type']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_merge_1['case_a'] = df_merge_1.apply(
        lambda x: (x["l_extendedprice"] * (1 - x["l_discount"])) if x["p_type"].startswith("PROMO") else 0, axis=1)
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['sumcase_a'] = [(df_merge_1.case_a).sum()]
    df_aggr_1['suml_extendedprice1l_discount'] = [((df_merge_1.l_extendedprice) * (1 - (df_merge_1.l_discount))).sum()]
    df_aggr_1 = df_aggr_1[['sumcase_a', 'suml_extendedprice1l_discount']]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['promo_revenue'] = ((100.00 * (df_aggr_1.sumcase_a)) / (df_aggr_1.suml_extendedprice1l_discount))
    df_aggr_2 = df_aggr_2[['promo_revenue']]
    df_limit_1 = df_aggr_2.head(1)
    return df_limit_1


def tpch_q15(lineitem, supplier):
    df_filter_1 = supplier[['s_suppkey', 's_name', 's_address', 's_phone']]
    df_filter_2 = lineitem[(lineitem.l_shipdate >= '1996-01-01') & (lineitem.l_shipdate < '1996-04-01') & (~lineitem.l_shipdate.isnull())]
    df_filter_2 = df_filter_2[['l_shipdate', 'l_suppkey', 'l_extendedprice', 'l_discount']]
    df_filter_2['supplier_no'] = df_filter_2.l_suppkey
    df_filter_2['before_1'] = ((df_filter_2.l_extendedprice) * (1 - (df_filter_2.l_discount)))
    df_group_1 = df_filter_2 \
        .groupby(['supplier_no'], sort=False) \
        .agg(
        total_revenue=("before_1", "sum"),
    )
    df_group_1 = df_group_1[['total_revenue']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_1 = df_filter_1.merge(df_group_1, left_on=['s_suppkey'], right_on=['supplier_no'], how="inner", sort=False)
    df_filter_3 = lineitem[(lineitem.l_shipdate >= '1996-01-01') & (lineitem.l_shipdate < '1996-04-01') & (~lineitem.l_shipdate.isnull())]
    df_filter_3 = df_filter_3[['l_shipdate', 'l_suppkey', 'l_extendedprice', 'l_discount']]
    df_filter_3['supplier_no'] = df_filter_3.l_suppkey
    df_filter_3['before_1'] = ((df_filter_3.l_extendedprice) * (1 - (df_filter_3.l_discount)))
    df_group_2 = df_filter_3 \
        .groupby(['supplier_no'], sort=False) \
        .agg(
        total_revenue=("before_1", "sum"),
    )
    df_group_2 = df_group_2[['total_revenue']]
    df_group_2 = df_group_2.reset_index()
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['maxtotal_revenue'] = [(df_group_2.total_revenue).max()]
    df_aggr_1 = df_aggr_1[['maxtotal_revenue']]
    df_limit_1 = df_aggr_1.head(1)
    df_merge_2 = df_merge_1.merge(df_limit_1, left_on=['total_revenue'], right_on=['maxtotal_revenue'], how="inner",
                                  sort=False)
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['s_suppkey'] = (df_merge_2.s_suppkey)
    df_aggr_2['s_name'] = (df_merge_2.s_name)
    df_aggr_2['s_address'] = (df_merge_2.s_address)
    df_aggr_2['s_phone'] = (df_merge_2.s_phone)
    df_aggr_2['total_revenue'] = (df_merge_2.total_revenue)
    df_aggr_2 = df_aggr_2[['s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue']]
    df_sort_1 = df_aggr_2.sort_values(by=['s_suppkey'], ascending=[True])
    df_limit_2 = df_sort_1.head(1)
    return df_limit_2


def tpch_q16(partsupp, part, supplier):
    df_filter_1 = partsupp[['ps_partkey', 'ps_suppkey']]
    df_filter_2 = part[part.p_size.isin([49, 14, 23, 45, 19, 3, 36, 9]) & (part.p_brand != 'Brand#45') & (
                part.p_type.str.contains("^MEDIUM POLISHED.*?$", regex=True) == False)]
    df_filter_2 = df_filter_2[['p_partkey', 'p_brand', 'p_type', 'p_size']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['ps_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_filter_3 = supplier[(supplier.s_comment.str.contains("^.*?Customer.*?Complaints.*?$", regex=True))]
    df_filter_3 = df_filter_3[['s_comment', 's_suppkey']]
    df_merge_2 = df_merge_1[~df_merge_1.ps_suppkey.isin(df_filter_3["s_suppkey"])]
    df_group_1 = df_merge_2 \
        .groupby(['p_brand', 'p_type', 'p_size'], sort=False) \
        .agg(
        supplier_cnt=("ps_suppkey", lambda x: x.nunique()),
    )
    df_group_1 = df_group_1[['supplier_cnt']]
    df_sort_1 = df_group_1.sort_values(by=['supplier_cnt', 'p_brand', 'p_type', 'p_size'],
                                       ascending=[False, True, True, True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q17(lineitem, part):
    df_filter_1 = lineitem[['l_partkey', 'l_quantity', 'l_extendedprice']]
    df_filter_2 = part[(part.p_brand == 'Brand#23') & (~part.p_brand.isnull()) & (part.p_container == 'MED BOX') & (
        ~part.p_container.isnull())]
    df_filter_2 = df_filter_2[['p_partkey', 'p_brand', 'p_container']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_filter_3 = lineitem[['l_partkey', 'l_quantity']]
    df_group_1 = df_filter_3 \
        .groupby(['l_partkey'], sort=False) \
        .agg(
        mean_l_quantity=("l_quantity", "mean"),
    )
    df_group_1['avgl_quantity'] = df_group_1.mean_l_quantity
    df_group_1 = df_group_1[['avgl_quantity']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_2 = df_merge_1.merge(df_group_1, left_on=['p_partkey'], right_on=['l_partkey'], how="inner", sort=False)
    df_merge_2 = df_merge_2[(df_merge_2.l_quantity < 0.200000 * df_merge_2.avgl_quantity)]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['suml_extendedprice'] = [(df_merge_2.l_extendedprice).sum()]
    df_aggr_1 = df_aggr_1[['suml_extendedprice']]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['avg_yearly'] = ((df_aggr_1.suml_extendedprice) / 7.0)
    df_aggr_2 = df_aggr_2[['avg_yearly']]
    df_limit_1 = df_aggr_2.head(1)
    return df_limit_1


def tpch_q18(lineitem, customer, orders):
    df_filter_1 = lineitem[['l_orderkey', 'l_quantity']]
    df_filter_2 = orders[['o_orderkey', 'o_custkey', 'o_orderdate', 'o_totalprice']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner",
                                   sort=False)
    df_filter_3 = customer[['c_custkey', 'c_name']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['o_custkey'], right_on=['c_custkey'], how="inner", sort=False)
    df_filter_4 = lineitem[['l_orderkey', 'l_quantity']]
    df_group_1 = df_filter_4 \
        .groupby(['l_orderkey'], sort=False) \
        .agg(
        sum_l_quantity=("l_quantity", "sum"),
    )
    df_group_1['suml_quantity'] = df_group_1.sum_l_quantity
    df_group_1 = df_group_1[df_group_1.suml_quantity > 300.000]
    df_group_1 = df_group_1[['suml_quantity']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_3 = df_merge_2[df_merge_2.o_orderkey.isin(df_group_1["l_orderkey"])]
    df_group_2 = df_merge_3 \
        .groupby(['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'], sort=False) \
        .agg(
        suml_quantity=("l_quantity", "sum"),
    )
    df_group_2 = df_group_2[['suml_quantity']]
    df_sort_1 = df_group_2.sort_values(by=['o_totalprice', 'o_orderdate'], ascending=[False, True])
    df_limit_1 = df_sort_1.head(100)
    return df_limit_1


def tpch_q19(lineitem, part):
    df_filter_1 = lineitem[(lineitem.l_shipinstruct == 'DELIVER IN PERSON') & (~lineitem.l_shipinstruct.isnull()) & lineitem.l_shipmode.isin(
        ['AIR', 'AIR REG'])]
    df_filter_1 = df_filter_1[
        ['l_partkey', 'l_quantity', 'l_shipmode', 'l_shipinstruct', 'l_extendedprice', 'l_discount']]
    df_filter_2 = part[['p_partkey', 'p_brand', 'p_container', 'p_size']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_merge_1 = df_merge_1[(df_merge_1.l_quantity <= 11) & (df_merge_1.p_size <= 5) & (
                df_merge_1.p_brand == 'Brand#12') & df_merge_1.p_container.isin(
        ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']) | (df_merge_1.l_quantity >= 10) & (df_merge_1.l_quantity <= 20) & (
                                        df_merge_1.p_size <= 10) & (
                                        df_merge_1.p_brand == 'Brand#23') & df_merge_1.p_container.isin(
        ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']) | (df_merge_1.l_quantity >= 20) & (
                                        df_merge_1.l_quantity <= 30) & (df_merge_1.p_size <= 15) & (
                                        df_merge_1.p_brand == 'Brand#34') & df_merge_1.p_container.isin(
        ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['l_extendedprice1l_discount'] = ((df_merge_1.l_extendedprice) * (1 - (df_merge_1.l_discount)))
    df_aggr_1 = df_aggr_1[['l_extendedprice1l_discount']]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['revenue'] = [(df_aggr_1.l_extendedprice1l_discount).sum()]
    df_aggr_2 = df_aggr_2[['revenue']]
    df_limit_1 = df_aggr_2.head(1)
    return df_limit_1


def tpch_q20(supplier, nation, partsupp, part, lineitem):
    df_filter_1 = supplier[['s_suppkey', 's_nationkey', 's_name', 's_address']]
    df_filter_2 = nation[(nation.n_name == 'CANADA') & (~nation.n_name.isnull())]
    df_filter_2 = df_filter_2[['n_nationkey', 'n_name']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_filter_3 = partsupp[['ps_partkey', 'ps_availqty', 'ps_suppkey']]
    df_filter_4 = part[(~part.p_name.isnull()) & (part.p_name.str.contains("^forest.*?$", regex=True))]
    df_filter_4 = df_filter_4[['p_name', 'p_partkey']]
    df_merge_2 = df_filter_3[df_filter_3.ps_partkey.isin(df_filter_4["p_partkey"])]
    df_filter_5 = lineitem[(lineitem.l_shipdate >= '1994-01-01') & (lineitem.l_shipdate < '1995-01-01') & (~lineitem.l_shipdate.isnull())]
    df_filter_5 = df_filter_5[['l_partkey', 'l_suppkey', 'l_shipdate', 'l_quantity']]
    df_group_1 = df_filter_5 \
        .groupby(['l_partkey', 'l_suppkey'], sort=False) \
        .agg(
        sum_l_quantity=("l_quantity", "sum"),
    )
    df_group_1['suml_quantity'] = df_group_1.sum_l_quantity
    df_group_1 = df_group_1[['suml_quantity']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_3 = df_merge_2.merge(df_group_1, left_on=['ps_partkey', 'ps_suppkey'], right_on=['l_partkey', 'l_suppkey'],
                                  how="inner", sort=False)
    df_merge_3 = df_merge_3[(df_merge_3.ps_availqty > 0.5 * df_merge_3.suml_quantity)]
    df_merge_4 = df_merge_1[df_merge_1.s_suppkey.isin(df_merge_3["ps_suppkey"])]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['s_name'] = (df_merge_4.s_name)
    df_aggr_1['s_address'] = (df_merge_4.s_address)
    df_aggr_1 = df_aggr_1[['s_name', 's_address']]
    df_sort_1 = df_aggr_1.sort_values(by=['s_name'], ascending=[True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1


def tpch_q21(supplier, lineitem, orders, nation):
    df_filter_1 = lineitem[(lineitem.l_receiptdate > lineitem.l_commitdate)]
    df_filter_1 = df_filter_1[['l_suppkey', 'l_orderkey', 'l_receiptdate', 'l_commitdate']]
    df_filter_2 = orders[(orders.o_orderstatus == 'F') & (~orders.o_orderstatus.isnull())]
    df_filter_2 = df_filter_2[['o_orderkey', 'o_orderstatus']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner",
                                   sort=False)
    df_filter_3 = supplier[['s_suppkey', 's_nationkey', 's_name']]
    df_filter_4 = nation[(nation.n_name == 'SAUDI ARABIA') & (~nation.n_name.isnull())]
    df_filter_4 = df_filter_4[['n_nationkey', 'n_name']]
    df_merge_2 = df_filter_3.merge(df_filter_4, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner",
                                   sort=False)
    df_merge_3 = df_merge_1.merge(df_merge_2, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_5 = lineitem[['l_orderkey', 'l_suppkey']]
    inner_cond = df_merge_3.merge(df_filter_5, left_on='l_orderkey', right_on='l_orderkey', how='inner', sort=False)
    inner_cond = inner_cond[inner_cond.l_suppkey_x != inner_cond.l_suppkey_y]['l_orderkey']
    df_merge_4 = df_merge_3[df_merge_3.l_orderkey.isin(inner_cond)]
    df_filter_6 = lineitem[(lineitem.l_receiptdate > lineitem.l_commitdate)]
    df_filter_6 = df_filter_6[['l_orderkey', 'l_suppkey', 'l_receiptdate', 'l_commitdate']]
    inner_cond = df_merge_4.merge(df_filter_6, left_on='l_orderkey', right_on='l_orderkey', how='inner', sort=False)
    inner_cond = inner_cond[inner_cond.l_suppkey_x != inner_cond.l_suppkey_y]['l_orderkey']
    df_merge_5 = df_merge_4.merge(inner_cond, left_on=['l_orderkey'], right_on=['l_orderkey'], how="outer",
                                  indicator=True, sort=False)
    df_merge_5 = df_merge_5[df_merge_5._merge == "left_only"]
    df_group_1 = df_merge_5 \
        .groupby(['s_name'], sort=False) \
        .agg(
        numwait=("s_name", "count"),
    )
    df_group_1 = df_group_1[['numwait']]
    df_sort_1 = df_group_1.sort_values(by=['numwait', 's_name'], ascending=[False, True])
    df_limit_1 = df_sort_1.head(100)
    return df_limit_1


def tpch_q22(customer, orders):
    df_filter_1 = customer[customer.c_phone.str.slice(0, 2).isin(['13', '31', '23', '29', '30', '18', '17'])]
    df_filter_1 = df_filter_1[['c_phone', 'c_acctbal', 'c_custkey']]
    df_filter_2 = customer[(customer.c_acctbal > 0) & (~customer.c_acctbal.isnull()) & customer.c_phone.str.slice(0, 2).isin(
        ['13', '31', '23', '29', '30', '18', '17'])]
    df_filter_2 = df_filter_2[['c_acctbal', 'c_phone']]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['avgc_acctbal'] = [(df_filter_2.c_acctbal).mean()]
    df_aggr_1 = df_aggr_1[['avgc_acctbal']]
    df_limit_1 = df_aggr_1.head(1)
    df_merge_1 = df_filter_1.merge(df_limit_1, how="cross", sort=False)
    df_merge_1 = df_merge_1[(df_merge_1.c_acctbal > df_merge_1.avgc_acctbal)]
    df_filter_3 = orders[['o_custkey']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['c_custkey'], right_on=['o_custkey'], how="outer",
                                  indicator=True, sort=False)
    df_merge_2 = df_merge_2[df_merge_2._merge == "left_only"]
    df_merge_2['cntrycode'] = df_merge_2.c_phone.str.slice(0, 2)
    df_group_1 = df_merge_2 \
        .groupby(['cntrycode'], sort=False) \
        .agg(
        numcust=("cntrycode", "count"),
        totacctbal=("c_acctbal", "sum"),
    )
    df_group_1 = df_group_1[['numcust', 'totacctbal']]
    df_sort_1 = df_group_1.sort_values(by=['cntrycode'], ascending=[True])
    df_limit_2 = df_sort_1.head(1)
    return df_limit_2
