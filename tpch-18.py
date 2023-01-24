"""
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > :1
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
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

    var1 = 251

    customer = pd.read_table(rf'{data_path}/customer.tbl', sep='|', index_col=False, header=None, names=pysdql.CUSTOMER_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)

    agg_l = lineitem.groupby(['l_orderkey'], as_index=False).filter(lambda x: x['l_quantity'].sum() > var1)
    agg_l.__columns.field = 'agg_l'

    r = customer.merge(orders, left_on='c_custkey', right_on='o_custkey')
    r = r.merge(lineitem, left_on='o_orderkey', right_on='l_orderkey')

    r = r[(r['o_orderkey'].isin(agg_l['l_orderkey']))]

    r = r.groupby(['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'], as_index=False)\
        .agg(l_quantity_sum=('l_quantity', 'sum'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-18').run(r).export().to()
