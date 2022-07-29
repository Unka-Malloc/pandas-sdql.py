"""
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date ':1'
	and o_orderdate < date ':1' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
"""
import pysdql
# Try replace pysdql with pandas to get result in pandas!
# import pandas as pd  # get answer in pandas
# import numpy as np  # use numpy.select() must use together with pandas
import pysdql as pd  # get answer in pysdql
import pysdqlnp as np  # use pysdqlnp.select() must use together with pysdql

# display all columns
pd.set_option('display.max_columns', None)
# display all rows
pd.set_option('display.max_rows', None)

if __name__ == '__main__':
    data_path = 'T:/UG4-Proj/datasets'
    sdql_database_path = r'T:/sdql'

    var1 = '1996-05-01'
    var2 = '1996-08-01'  # var1 + 3 month

    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None,names=pysdql.LINEITEM_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)

    sub_o = orders[(orders['o_orderdate'] >= var1) & (orders['o_orderdate'] < var2)]
    sub_o.columns.name = 'sub_o'

    r = sub_o.merge(lineitem, left_on='o_orderkey', right_on='l_orderkey')

    r['exists'] = np.select(
        [
            (r['l_commitdate'] < r['l_receiptdate'])
        ],
        [
            1
        ],
        default=0)
    r = r.groupby(['o_orderpriority', 'o_orderkey'], as_index=False).agg(exists=('exists', 'sum'))
    r = r[r['exists'] > 0]

    r = r.groupby(['o_orderpriority'], as_index=False).agg(order_count=('o_orderkey', 'count'))

    print(r)

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-4').run(r).export().to()
