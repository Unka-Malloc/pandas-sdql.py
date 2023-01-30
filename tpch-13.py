"""
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%:1%:2%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
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

    var1 = 'special'
    var2 = 'requests'

    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    customer = pd.read_table(rf'{data_path}/customer.tbl', sep='|', index_col=False, header=None, names=pysdql.CUSTOMER_COLS)

    orders['var1_index'] = orders['o_comment'].str.find(var1)
    orders['var2_index'] = orders['o_comment'].str.find(var2)

    sub_o = orders[~((orders['var1_index'] != -1) & (orders['var2_index'] != -1) & (orders['var1_index'] < orders['var2_index']))]
    sub_o.__columns.field = 'sub_o'

    # LEFT OUTER JOIN
    r = customer.merge(sub_o, how='left', left_on='c_custkey', right_on='o_custkey')

    c_orders = r.groupby(['c_custkey'], as_index=False).agg(c_count=('o_orderkey', 'count'))
    c_orders.__columns.field = 'c_orders'

    s = c_orders.groupby(['c_count'], as_index=False).agg(custdist=('c_custkey', 'count'))

    print(s)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-13').run(s).export().to()
