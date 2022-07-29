"""
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = ':1'
	and o_orderdate >= date ':2'
	and o_orderdate < date ':2' + interval '1' year
group by
	n_name
order by
	revenue desc
"""
import pysdql
# Try replace pysdql with pandas to get result in pandas!
# import pandas as pd  # get answer in pandas
import pysdql as pd  # get answer in pysdql

# display all columns
pd.set_option('display.max_columns', None)
# display all rows
pd.set_option('display.max_rows', None)

if __name__ == '__main__':
    data_path = 'T:/UG4-Proj/datasets'
    sdql_database_path = r'T:/sdql'

    var1 = 'MIDDLE EAST'
    var2 = '1995-01-01'
    var3 = '1996-01-01'  # var2 + 1 year

    customer = pd.read_table(rf'{data_path}/customer.tbl', sep='|', index_col=False, header=None, names=pysdql.CUSTOMER_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)
    nation = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=pysdql.NATION_COLS)
    region = pd.read_table(rf'{data_path}/region.tbl', sep='|', index_col=False, header=None, names=pysdql.REGION_COLS)

    sub_r = region[region['r_name'] == var1]
    sub_r.columns.name = 'sub_r'

    sub_o = orders[(orders['o_orderdate'] >= var2) & (orders['o_orderdate'] < var3)]
    sub_o.columns.name = 'sub_o'

    r = sub_r.merge(right=nation, left_on='r_regionkey', right_on='n_regionkey')
    r = r.merge(right=customer, left_on='n_nationkey', right_on='c_nationkey')
    r = r.merge(right=sub_o, left_on='c_custkey', right_on='o_custkey')
    r = r.merge(right=lineitem, left_on='o_orderkey', right_on='l_orderkey')
    r = r.merge(right=supplier,
                left_on=['c_nationkey', 'n_nationkey', 'l_suppkey'],
                right_on=['s_nationkey', 's_nationkey', 's_suppkey'])

    r['value'] = r['l_extendedprice'] * (1 - r['l_discount'])

    r = r.groupby(['n_name'], as_index=False).agg(revenue=('value', 'sum'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-5').run(r).export().to()
