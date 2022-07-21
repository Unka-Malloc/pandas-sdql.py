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
from datetime import datetime, timedelta

import pysdql

if __name__ == '__main__':
    var1 = '1996-05-01'
    var2 = '1996-08-01'  # var1 + 3 month

    db_driver = pysdql.db_driver(db_path=r'T:/sdql', name='tpch-4')

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)

    r = lineitem[(lineitem['l_commitdate'] < lineitem['l_receiptdate'])]
    r = r.merge(orders, on=(r['l_orderkey'] == orders['o_orderkey']))

    s = orders[(orders['o_orderdate'] >= var1) & (orders['o_orderdate'] < var2) & r.exists()]
    s = s.groupby(['o_orderpriority']).aggr(order_count=('*', 'count'))

    db_driver.run(s).export().to()
