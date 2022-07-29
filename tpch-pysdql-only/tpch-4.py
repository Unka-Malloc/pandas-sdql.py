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

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', names=pysdql.LINEITEM_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', names=pysdql.ORDERS_COLS)

    sub_l = lineitem[(lineitem['l_commitdate'] < lineitem['l_receiptdate'])].rename('sub_l')
    sub_o = orders[(orders['o_orderdate'] >= var1) & (orders['o_orderdate'] < var2)].rename('sub_o')

    r = sub_o[sub_o['o_orderkey'].exists(sub_l['l_orderkey'])]

    r = r.groupby(['o_orderpriority']).agg(order_count=('*', 'count'))

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-4').run(r).export().to()
