"""
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = ':1'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date ':2'
	and l_shipdate > date ':2'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
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

    var1 = 'BUILDING'
    var2 = '1995-03-22'

    customer = pd.read_table(rf'{data_path}/customer.tbl', sep='|', index_col=False, header=None, names=pysdql.CUSTOMER_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)

    sub_c = customer[customer['c_mktsegment'] == var1]
    sub_c.columns.name = 'sub_c'

    sub_o = orders[orders['o_orderdate'] < var2]
    sub_o.columns.name = 'sub_o'

    sub_l = lineitem[lineitem['l_shipdate'] > var2]
    sub_l.columns.name = 'sub_l'

    # 1M - 43s (without sub-table)
    # 1M - 28s (with sub-table)
    r = sub_c.merge(sub_o, left_on='c_custkey', right_on='o_custkey')
    r = r.merge(sub_l, left_on='o_orderkey', right_on='l_orderkey')

    # 1M - 73s (without sub-table)
    # 1M - 24s (with sub-table)
    # r = sub_c.merge(sub_o, on=(sub_c['c_custkey'] == sub_o['o_custkey']))
    # r = r.merge(sub_l, on=(r['o_orderkey'] == sub_l['l_orderkey']))

    r['value'] = r['l_extendedprice'] * (1 - r['l_discount'])

    r = r.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority'], as_index=False) \
        .agg(revenue=('value', 'sum'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-3').run(r).export().to()
