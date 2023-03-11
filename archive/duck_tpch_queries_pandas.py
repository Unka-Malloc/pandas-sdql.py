import pandas as pd


# Lineitem

l_columnnames = ["L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX",
                "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"]

for i in range(len(l_columnnames)):
    l_columnnames[i] = l_columnnames[i].lower()

l_data_types = {
    'l_orderkey': int,
    'l_partkey': int,
    'l_suppkey': int,
    'l_linenumber': int,
    'l_quantity': float,
    'l_extendedprice': float,
    'l_discount': float,
    'l_tax': float,
    'l_returnflag': str,
    'l_linestatus': str,
    'l_shipinstruct': str,
    'l_shipmode': str,
    'l_comment': str
}

l_parse_dates = ['l_shipdate', 'l_commitdate', 'l_receiptdate']

# Don't set indexes, as we can't access them with Pandas selection!
li = pd.read_table("data_storage/lineitem.tbl.csv", sep="|", names=l_columnnames, dtype=l_data_types, parse_dates=l_parse_dates)

# Order

o_columnnames = ["O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"]

for i in range(len(o_columnnames)):
    o_columnnames[i] = o_columnnames[i].lower()
    
o_data_types = {
    'o_orderkey': int,
    'o_custkey': int,
    'o_orderstatus': str,
    'o_totalprice': float,
    'o_orderpriority': str,
    'o_clerk': str,
    'o_shippriority': int,
    'o_comment': str
}

o_parse_dates = ['o_orderdate']

# Don't set indexes, as we can't access them with Pandas selection!
ord = pd.read_table("data_storage/orders.tbl.csv", sep="|", names=o_columnnames, dtype=o_data_types, parse_dates=o_parse_dates)

# Customer

c_columnnames = ["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"]

for i in range(len(c_columnnames)):
    c_columnnames[i] = c_columnnames[i].lower()
    
c_data_types = {
    'c_custkey': int,
    'c_name': str,
    'c_address': str,
    'c_nationkey': int,
    'c_phone': str,
    'c_acctbal': float,
    'c_mktsegment': str,
    'c_comment': str
}

c_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
cu = pd.read_table("data_storage/customer.tbl.csv", sep="|", names=c_columnnames, dtype=c_data_types, parse_dates=c_parse_dates)

# Part

p_columnnames = ["P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"]

for i in range(len(p_columnnames)):
    p_columnnames[i] = p_columnnames[i].lower()
    
p_data_types = {
    'p_partkey': int, 
    'p_name': str,
    'p_mfgr': str,
    'p_brand': str,
    'p_type': str,
    'p_size': int,
    'p_container': str,
    'p_retailprice': float,
    'p_comment': str
}

p_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
pa = pd.read_table("data_storage/part.tbl.csv", sep="|", names=p_columnnames, dtype=p_data_types, parse_dates=p_parse_dates)

# Nation

n_columnnames = ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"]

for i in range(len(n_columnnames)):
    n_columnnames[i] = n_columnnames[i].lower()
    
n_data_types = {
    'n_nationkey': int,
    'n_name': str,
    'n_regionkey': int,
    'n_comment': str,
}

n_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
na = pd.read_table("data_storage/nation.tbl.csv", sep="|", names=n_columnnames, dtype=n_data_types, parse_dates=n_parse_dates)

# Supplier

s_columnnames = ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"]

for i in range(len(s_columnnames)):
    s_columnnames[i] = s_columnnames[i].lower()

s_data_types = {
    's_suppkey': int,
    's_name': str,
    's_address': str,
    's_nationkey': int,
    's_phone': str,
    's_acctbal': float,
    's_comment': str
}

s_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
su = pd.read_table("data_storage/supplier.tbl.csv", sep="|", names=s_columnnames, dtype=s_data_types, parse_dates=s_parse_dates)

# Partsupp

ps_columnnames = ["PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"]

for i in range(len(ps_columnnames)):
    ps_columnnames[i] = ps_columnnames[i].lower()

ps_data_types = {
    'ps_partkey': int,
    'ps_suppkey': int,
    'ps_availqty': int,
    'ps_supplycost': float,
    'ps_comment': str
}

ps_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
ps = pd.read_table("data_storage/partsupp.tbl.csv", sep="|", names=ps_columnnames, dtype=ps_data_types, parse_dates=ps_parse_dates)

# Region

r_columnnames = ["R_REGIONKEY", "R_NAME", "R_COMMENT"]

for i in range(len(r_columnnames)):
    r_columnnames[i] = r_columnnames[i].lower()

r_data_types = {
    'r_regionkey': int,
    'r_name': str,
    'r_comment': str
}

r_parse_dates = []

# Don't set indexes, as we can't access them with Pandas selection!
re = pd.read_table("data_storage/region.tbl.csv", sep="|", names=r_columnnames, dtype=r_data_types, parse_dates=r_parse_dates)


def q1():
    df_filter_1 = li[(li.l_shipdate<='1998-09-02') & (~li.l_shipdate.isnull())]
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

def q2():
    df_filter_1 = ps[['ps_partkey', 'ps_suppkey', 'ps_supplycost']]
    df_filter_2 = su[['s_suppkey', 's_nationkey', 's_acctbal', 's_name', 's_address', 's_phone', 's_comment']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_3 = na[['n_nationkey', 'n_regionkey', 'n_name']]
    df_filter_4 = re[(re.r_name == 'EUROPE') & (~re.r_name.isnull())]
    df_filter_4 = df_filter_4[['r_regionkey', 'r_name']]
    df_merge_2 = df_filter_3.merge(df_filter_4, left_on=['n_regionkey'], right_on=['r_regionkey'], how="inner", sort=False)
    df_merge_3 = df_merge_1.merge(df_merge_2, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_filter_5 = pa[(pa.p_size == 15) & (~pa.p_size.isnull()) & (pa.p_type.str.contains("^.*?BRASS$",regex=True))]
    df_filter_5 = df_filter_5[['p_partkey', 'p_size', 'p_type', 'p_mfgr']]
    df_merge_4 = df_merge_3.merge(df_filter_5, left_on=['ps_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_filter_6 = ps[['ps_partkey', 'ps_suppkey', 'ps_supplycost']]
    df_filter_7 = su[['s_suppkey', 's_nationkey']]
    df_merge_5 = df_filter_6.merge(df_filter_7, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_8 = na[['n_nationkey', 'n_regionkey']]
    df_merge_6 = df_merge_5.merge(df_filter_8, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_filter_9 = re[(re.r_name == 'EUROPE') & (~re.r_name.isnull())]
    df_filter_9 = df_filter_9[['r_regionkey', 'r_name']]
    df_merge_7 = df_merge_6.merge(df_filter_9, left_on=['n_regionkey'], right_on=['r_regionkey'], how="inner", sort=False)
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
    df_sort_1 = df_aggr_1.sort_values(by=['s_acctbal', 'n_name', 's_name', 'p_partkey'], ascending=[False, True, True, True])
    df_limit_1 = df_sort_1.head(100)
    return df_limit_1

def q3():
    df_filter_1 = li[(li.l_shipdate>'1995-03-15') & (~li.l_shipdate.isnull())]
    df_filter_1 = df_filter_1[['l_orderkey', 'l_shipdate', 'l_extendedprice', 'l_discount']]
    df_filter_2 = ord[(ord.o_orderdate<'1995-03-15') & (~ord.o_orderdate.isnull())]
    df_filter_2 = df_filter_2[['o_custkey', 'o_orderkey', 'o_orderdate', 'o_shippriority']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_3 = cu[(cu.c_mktsegment == 'BUILDING') & (~cu.c_mktsegment.isnull())]
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

def q4():
    df_filter_1 = ord[(ord.o_orderdate>='1993-07-01') & (ord.o_orderdate<'1993-10-01') & (~ord.o_orderdate.isnull())]
    df_filter_1 = df_filter_1[['o_orderdate', 'o_orderkey', 'o_orderpriority']]
    df_filter_2 = li[(li.l_commitdate < li.l_receiptdate)]
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

def q5():
    df_filter_1 = li[['l_orderkey', 'l_suppkey', 'l_extendedprice', 'l_discount']]
    df_filter_2 = su[['s_suppkey', 's_nationkey']]
    df_filter_3 = na[['n_nationkey', 'n_regionkey', 'n_name']]
    df_filter_4 = re[(re.r_name == 'ASIA') & (~re.r_name.isnull())]
    df_filter_4 = df_filter_4[['r_regionkey', 'r_name']]
    df_merge_1 = df_filter_3.merge(df_filter_4, left_on=['n_regionkey'], right_on=['r_regionkey'], how="inner", sort=False)
    df_merge_2 = df_filter_2.merge(df_merge_1, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_3 = df_filter_1.merge(df_merge_2, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_5 = ord[(ord.o_orderdate>='1994-01-01') & (ord.o_orderdate<'1995-01-01') & (~ord.o_orderdate.isnull())]
    df_filter_5 = df_filter_5[['o_custkey', 'o_orderkey', 'o_orderdate']]
    df_merge_4 = df_merge_3.merge(df_filter_5, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_6 = cu[['c_custkey', 'c_nationkey']]
    df_merge_5 = df_merge_4.merge(df_filter_6, left_on=['n_nationkey', 's_nationkey', 'o_custkey'], right_on=['c_nationkey', 'c_nationkey', 'c_custkey'], how="inner", sort=False)
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

def q6():
    df_filter_1 = li[(li.l_shipdate>='1994-01-01') & (li.l_shipdate<'1995-01-01') & (~li.l_shipdate.isnull()) & (li.l_discount>=0.050) & (li.l_discount<=0.070) & (~li.l_discount.isnull()) & (li.l_quantity<24) & (~li.l_quantity.isnull())]
    df_filter_1 = df_filter_1[['l_shipdate', 'l_discount', 'l_quantity', 'l_extendedprice']]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['l_extendedpricel_discount'] = ((df_filter_1.l_extendedprice) * (df_filter_1.l_discount))
    df_aggr_1 = df_aggr_1[['l_extendedpricel_discount']]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['revenue'] = [(df_aggr_1.l_extendedpricel_discount).sum()]
    df_aggr_2 = df_aggr_2[['revenue']]
    df_limit_1 = df_aggr_2.head(1)
    return df_limit_1

def q7():
    df_filter_1 = li[(li.l_shipdate>='1995-01-01') & (li.l_shipdate<='1996-12-31') & (~li.l_shipdate.isnull())]
    df_filter_1 = df_filter_1[['l_suppkey', 'l_orderkey', 'l_shipdate', 'l_extendedprice', 'l_discount']]
    df_filter_2 = ord[['o_orderkey', 'o_custkey']]
    df_filter_3 = cu[['c_custkey', 'c_nationkey']]
    df_filter_4 = na[['n_nationkey', 'n_name']]
    df_merge_1 = df_filter_3.merge(df_filter_4, left_on=['c_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_2 = df_filter_2.merge(df_merge_1, left_on=['o_custkey'], right_on=['c_custkey'], how="inner", sort=False)
    df_merge_3 = df_filter_1.merge(df_merge_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_5 = su[['s_suppkey', 's_nationkey']]
    df_filter_6 = na[['n_nationkey', 'n_name']]
    df_merge_4 = df_filter_5.merge(df_filter_6, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_5 = df_merge_3.merge(df_merge_4, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_merge_5 = df_merge_5[(df_merge_5.n_name_x == 'FRANCE') & (df_merge_5.n_name_y == 'GERMANY') | (df_merge_5.n_name_x == 'GERMANY') & (df_merge_5.n_name_y == 'FRANCE')]
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

def q8():
    df_filter_1 = li[['l_partkey', 'l_suppkey', 'l_orderkey', 'l_extendedprice', 'l_discount']]
    df_filter_2 = ord[(ord.o_orderdate>='1995-01-01') & (ord.o_orderdate<='1996-12-31') & (~ord.o_orderdate.isnull())]
    df_filter_2 = df_filter_2[['o_orderkey', 'o_custkey', 'o_orderdate']]
    df_filter_3 = cu[['c_custkey', 'c_nationkey']]
    df_filter_4 = na[['n_nationkey', 'n_regionkey']]
    df_filter_5 = re[(re.r_name == 'AMERICA') & (~re.r_name.isnull())]
    df_filter_5 = df_filter_5[['r_regionkey', 'r_name']]
    df_merge_1 = df_filter_4.merge(df_filter_5, left_on=['n_regionkey'], right_on=['r_regionkey'], how="inner", sort=False)
    df_merge_2 = df_filter_3.merge(df_merge_1, left_on=['c_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_3 = df_filter_2.merge(df_merge_2, left_on=['o_custkey'], right_on=['c_custkey'], how="inner", sort=False)
    df_merge_4 = df_filter_1.merge(df_merge_3, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_6 = su[['s_suppkey', 's_nationkey']]
    df_filter_7 = na[['n_nationkey', 'n_name']]
    df_merge_5 = df_filter_6.merge(df_filter_7, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_6 = df_merge_4.merge(df_merge_5, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_8 = pa[(pa.p_type == 'ECONOMY ANODIZED STEEL') & (~pa.p_type.isnull())]
    df_filter_8 = df_filter_8[['p_partkey', 'p_type']]
    df_merge_7 = df_merge_6.merge(df_filter_8, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_merge_7['case_a'] = df_merge_7.apply(lambda x: ( x["l_extendedprice"] * ( 1 - x["l_discount"] )) if ( x["n_name"] == 'BRAZIL' ) else 0, axis=1)
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

def q9():
    df_filter_1 = li[['l_suppkey', 'l_partkey', 'l_orderkey', 'l_extendedprice', 'l_discount', 'l_quantity']]
    df_filter_2 = ps[['ps_suppkey', 'ps_partkey', 'ps_supplycost']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_suppkey', 'l_partkey'], right_on=['ps_suppkey', 'ps_partkey'], how="inner", sort=False)
    df_filter_3 = ord[['o_orderkey', 'o_orderdate']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_4 = su[['s_suppkey', 's_nationkey']]
    df_filter_5 = na[['n_nationkey', 'n_name']]
    df_merge_3 = df_filter_4.merge(df_filter_5, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_4 = df_merge_2.merge(df_merge_3, left_on=['ps_suppkey', 'l_suppkey'], right_on=['s_suppkey', 's_suppkey'], how="inner", sort=False)
    df_filter_6 = pa[(pa.p_name.str.contains("^.*?green.*?$",regex=True))]
    df_filter_6 = df_filter_6[['p_partkey', 'p_name']]
    df_merge_5 = df_merge_4.merge(df_filter_6, left_on=['ps_partkey', 'l_partkey'], right_on=['p_partkey', 'p_partkey'], how="inner", sort=False)
    df_merge_5['nation'] = df_merge_5.n_name
    df_merge_5['o_year'] = df_merge_5.o_orderdate.dt.year
    df_merge_5['amount'] = (((df_merge_5.l_extendedprice) * (1 - (df_merge_5.l_discount))) - ((df_merge_5.ps_supplycost) * (df_merge_5.l_quantity)))
    df_group_1 = df_merge_5 \
        .groupby(['nation', 'o_year'], sort=False) \
        .agg(
            sum_profit=("amount", "sum"),
        )
    df_group_1 = df_group_1[['sum_profit']]
    df_sort_1 = df_group_1.sort_values(by=['nation', 'o_year'], ascending=[True, False])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1

def q10():
    df_filter_1 = li[(li.l_returnflag == 'R') & (~li.l_returnflag.isnull())]
    df_filter_1 = df_filter_1[['l_orderkey', 'l_returnflag', 'l_extendedprice', 'l_discount']]
    df_filter_2 = ord[(ord.o_orderdate>='1993-10-01') & (ord.o_orderdate<'1994-01-01') & (~ord.o_orderdate.isnull())]
    df_filter_2 = df_filter_2[['o_custkey', 'o_orderkey', 'o_orderdate']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_3 = cu[['c_custkey', 'c_nationkey', 'c_name', 'c_acctbal', 'c_phone', 'c_address', 'c_comment']]
    df_filter_4 = na[['n_nationkey', 'n_name']]
    df_merge_2 = df_filter_3.merge(df_filter_4, left_on=['c_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
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
    df_aggr_1 = df_aggr_1[['c_custkey', 'c_name', 'revenue', 'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment']]
    df_sort_1 = df_aggr_1.sort_values(by=['revenue'], ascending=[False])
    df_limit_1 = df_sort_1.head(20)
    return df_limit_1

def q11():
    df_filter_1 = ps[['ps_suppkey', 'ps_partkey', 'ps_supplycost', 'ps_availqty']]
    df_filter_2 = su[['s_suppkey', 's_nationkey']]
    df_filter_3 = na[(na.n_name == 'GERMANY') & (~na.n_name.isnull())]
    df_filter_3 = df_filter_3[['n_nationkey', 'n_name']]
    df_merge_1 = df_filter_2.merge(df_filter_3, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_2 = df_filter_1.merge(df_merge_1, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_merge_2['before_1'] = ((df_merge_2.ps_supplycost) * (df_merge_2.ps_availqty))
    df_group_1 = df_merge_2 \
        .groupby(['ps_partkey'], sort=False) \
        .agg(
            value=("before_1", "sum"),
        )
    df_group_1 = df_group_1[['value']]
    df_filter_4 = ps[['ps_suppkey', 'ps_supplycost', 'ps_availqty']]
    df_filter_5 = su[['s_suppkey', 's_nationkey']]
    df_filter_6 = na[(na.n_name == 'GERMANY') & (~na.n_name.isnull())]
    df_filter_6 = df_filter_6[['n_nationkey', 'n_name']]
    df_merge_3 = df_filter_5.merge(df_filter_6, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_4 = df_filter_4.merge(df_merge_3, left_on=['ps_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['sumps_supplycostps_availqty00001'] = [(((df_merge_4.ps_supplycost) * (df_merge_4.ps_availqty)).sum() * 0.0001)]
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

def q12():
    df_filter_1 = li[(li.l_shipdate<'1995-01-01') & (~li.l_shipdate.isnull()) & (li.l_receiptdate>='1994-01-01') & (li.l_receiptdate<'1995-01-01') & (~li.l_receiptdate.isnull()) & (li.l_commitdate<'1995-01-01') & (~li.l_commitdate.isnull()) & (li.l_commitdate < li.l_receiptdate) & (li.l_shipdate < li.l_commitdate) & li.l_shipmode.isin(['MAIL', 'SHIP'])]
    df_filter_1 = df_filter_1[['l_orderkey', 'l_shipmode', 'l_commitdate', 'l_receiptdate', 'l_shipdate']]
    df_filter_2 = ord[['o_orderkey', 'o_orderpriority']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_merge_1['case_a'] = df_merge_1.apply(lambda x: ( 1 ) if ( x['o_orderpriority'] == '1-URGENT' ) | ( x['o_orderpriority'] == '2-HIGH' ) else 0, axis=1)
    df_merge_1['case_b'] = df_merge_1.apply(lambda x: ( 1 ) if ( x['o_orderpriority'] != '1-URGENT' ) & ( x['o_orderpriority'] != '2-HIGH' ) else 0, axis=1)
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

def q13():
    df_filter_1 = ord[(ord.o_comment.str.contains("^.*?special.*?requests.*?$",regex=True) == False)]
    df_filter_1 = df_filter_1[['o_custkey', 'o_comment', 'o_orderkey']]
    df_filter_2 = cu[['c_custkey']]
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

def q14():
    df_filter_1 = li[(li.l_shipdate>='1995-09-01') & (li.l_shipdate<'1995-10-01') & (~li.l_shipdate.isnull())]
    df_filter_1 = df_filter_1[['l_partkey', 'l_shipdate', 'l_extendedprice', 'l_discount']]
    df_filter_2 = pa[['p_partkey', 'p_type']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_merge_1['case_a'] = df_merge_1.apply(lambda x: ( x["l_extendedprice"] * ( 1 - x["l_discount"] )) if x["p_type"].startswith("PROMO") else 0, axis=1)
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['sumcase_a'] = [(df_merge_1.case_a).sum()]
    df_aggr_1['suml_extendedprice1l_discount'] = [((df_merge_1.l_extendedprice) * (1 - (df_merge_1.l_discount))).sum()]
    df_aggr_1 = df_aggr_1[['sumcase_a', 'suml_extendedprice1l_discount']]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['promo_revenue'] = ((100.00 * (df_aggr_1.sumcase_a)) / (df_aggr_1.suml_extendedprice1l_discount))
    df_aggr_2 = df_aggr_2[['promo_revenue']]
    df_limit_1 = df_aggr_2.head(1)
    return df_limit_1

def q15():
    df_filter_1 = su[['s_suppkey', 's_name', 's_address', 's_phone']]
    df_filter_2 = li[(li.l_shipdate>='1996-01-01') & (li.l_shipdate<'1996-04-01') & (~li.l_shipdate.isnull())]
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
    df_filter_3 = li[(li.l_shipdate>='1996-01-01') & (li.l_shipdate<'1996-04-01') & (~li.l_shipdate.isnull())]
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
    df_merge_2 = df_merge_1.merge(df_limit_1, left_on=['total_revenue'], right_on=['maxtotal_revenue'], how="inner", sort=False)
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

def q16():
    df_filter_1 = ps[['ps_partkey', 'ps_suppkey']]
    df_filter_2 = pa[pa.p_size.isin([49, 14, 23, 45, 19, 3, 36, 9]) & (pa.p_brand != 'Brand#45') & (pa.p_type.str.contains("^MEDIUM POLISHED.*?$",regex=True) == False)]
    df_filter_2 = df_filter_2[['p_partkey', 'p_brand', 'p_type', 'p_size']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['ps_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_filter_3 = su[(su.s_comment.str.contains("^.*?Customer.*?Complaints.*?$",regex=True))]
    df_filter_3 = df_filter_3[['s_comment', 's_suppkey']]
    df_merge_2 = df_merge_1[~df_merge_1.ps_suppkey.isin(df_filter_3["s_suppkey"])]
    df_group_1 = df_merge_2 \
        .groupby(['p_brand', 'p_type', 'p_size'], sort=False) \
        .agg(
            supplier_cnt=("ps_suppkey", lambda x: x.nunique()),
        )
    df_group_1 = df_group_1[['supplier_cnt']]
    df_sort_1 = df_group_1.sort_values(by=['supplier_cnt', 'p_brand', 'p_type', 'p_size'], ascending=[False, True, True, True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1

def q17():
    df_filter_1 = li[['l_partkey', 'l_quantity', 'l_extendedprice']]
    df_filter_2 = pa[(pa.p_brand == 'Brand#23') & (~pa.p_brand.isnull()) & (pa.p_container == 'MED BOX') & (~pa.p_container.isnull())]
    df_filter_2 = df_filter_2[['p_partkey', 'p_brand', 'p_container']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_filter_3 = li[['l_partkey', 'l_quantity']]
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

def q18():
    df_filter_1 = li[['l_orderkey', 'l_quantity']]
    df_filter_2 = ord[['o_orderkey', 'o_custkey', 'o_orderdate', 'o_totalprice']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_3 = cu[['c_custkey', 'c_name']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['o_custkey'], right_on=['c_custkey'], how="inner", sort=False)
    df_filter_4 = li[['l_orderkey', 'l_quantity']]
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

def q19():
    df_filter_1 = li[(li.l_shipinstruct == 'DELIVER IN PERSON') & (~li.l_shipinstruct.isnull()) & li.l_shipmode.isin(['AIR', 'AIR REG'])]
    df_filter_1 = df_filter_1[['l_partkey', 'l_quantity', 'l_shipmode', 'l_shipinstruct', 'l_extendedprice', 'l_discount']]
    df_filter_2 = pa[['p_partkey', 'p_brand', 'p_container', 'p_size']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_partkey'], right_on=['p_partkey'], how="inner", sort=False)
    df_merge_1 = df_merge_1[(df_merge_1.l_quantity <= 11) & (df_merge_1.p_size <= 5) & (df_merge_1.p_brand == 'Brand#12') & df_merge_1.p_container.isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']) | (df_merge_1.l_quantity >= 10) & (df_merge_1.l_quantity <= 20) & (df_merge_1.p_size <= 10) & (df_merge_1.p_brand == 'Brand#23') & df_merge_1.p_container.isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']) | (df_merge_1.l_quantity >= 20) & (df_merge_1.l_quantity <= 30) & (df_merge_1.p_size <= 15) & (df_merge_1.p_brand == 'Brand#34') & df_merge_1.p_container.isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['l_extendedprice1l_discount'] = ((df_merge_1.l_extendedprice) * (1 - (df_merge_1.l_discount)))
    df_aggr_1 = df_aggr_1[['l_extendedprice1l_discount']]
    df_aggr_2 = pd.DataFrame()
    df_aggr_2['revenue'] = [(df_aggr_1.l_extendedprice1l_discount).sum()]
    df_aggr_2 = df_aggr_2[['revenue']]
    df_limit_1 = df_aggr_2.head(1)
    return df_limit_1

def q20():
    df_filter_1 = su[['s_suppkey', 's_nationkey', 's_name', 's_address']]
    df_filter_2 = na[(na.n_name == 'CANADA') & (~na.n_name.isnull())]
    df_filter_2 = df_filter_2[['n_nationkey', 'n_name']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_filter_3 = ps[['ps_partkey', 'ps_availqty', 'ps_suppkey']]
    df_filter_4 = pa[(~pa.p_name.isnull()) & (pa.p_name.str.contains("^forest.*?$",regex=True))]
    df_filter_4 = df_filter_4[['p_name', 'p_partkey']]
    df_merge_2 = df_filter_3[df_filter_3.ps_partkey.isin(df_filter_4["p_partkey"])]
    df_filter_5 = li[(li.l_shipdate>='1994-01-01') & (li.l_shipdate<'1995-01-01') & (~li.l_shipdate.isnull())]
    df_filter_5 = df_filter_5[['l_partkey', 'l_suppkey', 'l_shipdate', 'l_quantity']]
    df_group_1 = df_filter_5 \
        .groupby(['l_partkey', 'l_suppkey'], sort=False) \
        .agg(
            sum_l_quantity=("l_quantity", "sum"),
        )
    df_group_1['suml_quantity'] = df_group_1.sum_l_quantity
    df_group_1 = df_group_1[['suml_quantity']]
    df_group_1 = df_group_1.reset_index(level=0)
    df_merge_3 = df_merge_2.merge(df_group_1, left_on=['ps_partkey', 'ps_suppkey'], right_on=['l_partkey', 'l_suppkey'], how="inner", sort=False)
    df_merge_3 = df_merge_3[(df_merge_3.ps_availqty > 0.5 * df_merge_3.suml_quantity)]
    df_merge_4 = df_merge_1[df_merge_1.s_suppkey.isin(df_merge_3["ps_suppkey"])]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['s_name'] = (df_merge_4.s_name)
    df_aggr_1['s_address'] = (df_merge_4.s_address)
    df_aggr_1 = df_aggr_1[['s_name', 's_address']]
    df_sort_1 = df_aggr_1.sort_values(by=['s_name'], ascending=[True])
    df_limit_1 = df_sort_1.head(1)
    return df_limit_1

def q21():
    df_filter_1 = li[(li.l_receiptdate > li.l_commitdate)]
    df_filter_1 = df_filter_1[['l_suppkey', 'l_orderkey', 'l_receiptdate', 'l_commitdate']]
    df_filter_2 = ord[(ord.o_orderstatus == 'F') & (~ord.o_orderstatus.isnull())]
    df_filter_2 = df_filter_2[['o_orderkey', 'o_orderstatus']]
    df_merge_1 = df_filter_1.merge(df_filter_2, left_on=['l_orderkey'], right_on=['o_orderkey'], how="inner", sort=False)
    df_filter_3 = su[['s_suppkey', 's_nationkey', 's_name']]
    df_filter_4 = na[(na.n_name == 'SAUDI ARABIA') & (~na.n_name.isnull())]
    df_filter_4 = df_filter_4[['n_nationkey', 'n_name']]
    df_merge_2 = df_filter_3.merge(df_filter_4, left_on=['s_nationkey'], right_on=['n_nationkey'], how="inner", sort=False)
    df_merge_3 = df_merge_1.merge(df_merge_2, left_on=['l_suppkey'], right_on=['s_suppkey'], how="inner", sort=False)
    df_filter_5 = li[['l_orderkey', 'l_suppkey']]
    inner_cond = df_merge_3.merge(df_filter_5, left_on='l_orderkey', right_on='l_orderkey', how='inner', sort=False)
    inner_cond = inner_cond[inner_cond.l_suppkey_x != inner_cond.l_suppkey_y]['l_orderkey']
    df_merge_4 = df_merge_3[df_merge_3.l_orderkey.isin(inner_cond)]
    df_filter_6 = li[(li.l_receiptdate > li.l_commitdate)]
    df_filter_6 = df_filter_6[['l_orderkey', 'l_suppkey', 'l_receiptdate', 'l_commitdate']]
    inner_cond = df_merge_4.merge(df_filter_6, left_on='l_orderkey', right_on='l_orderkey', how='inner', sort=False)
    inner_cond = inner_cond[inner_cond.l_suppkey_x != inner_cond.l_suppkey_y]['l_orderkey']
    df_merge_5 = df_merge_4.merge(inner_cond, left_on=['l_orderkey'], right_on=['l_orderkey'], how="outer", indicator=True, sort=False)
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

def q22():
    df_filter_1 = cu[cu.c_phone.str.slice(0, 2).isin(['13', '31', '23', '29', '30', '18', '17'])]
    df_filter_1 = df_filter_1[['c_phone', 'c_acctbal', 'c_custkey']]
    df_filter_2 = cu[(cu.c_acctbal>0) & (~cu.c_acctbal.isnull()) & cu.c_phone.str.slice(0, 2).isin(['13', '31', '23', '29', '30', '18', '17'])]
    df_filter_2 = df_filter_2[['c_acctbal', 'c_phone']]
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['avgc_acctbal'] = [(df_filter_2.c_acctbal).mean()]
    df_aggr_1 = df_aggr_1[['avgc_acctbal']]
    df_limit_1 = df_aggr_1.head(1)
    df_merge_1 = df_filter_1.merge(df_limit_1, how="cross", sort=False)
    df_merge_1 = df_merge_1[(df_merge_1.c_acctbal > df_merge_1.avgc_acctbal)]
    df_filter_3 = ord[['o_custkey']]
    df_merge_2 = df_merge_1.merge(df_filter_3, left_on=['c_custkey'], right_on=['o_custkey'], how="outer", indicator=True, sort=False)
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


# =====================

q1()
q2()
q3()
q4()
q5()
q6()
q7()
q8()
q9()
q10()
q11()
q12()
q13()
q14()
q15()
q16()
q17()
q18()
q19()
q20()
q21()
q22()

