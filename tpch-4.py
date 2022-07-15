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

if __name__ == '__main__':
    db_driver = pysdql.driver(db_path=r'T:/sdql')

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)

    r = pysdql.merge(lineitem, orders,
                     on=(lineitem['l_orderkey'] == orders['o_orderkey'])
                     )[(lineitem['l_commitdate'] < lineitem['l_receiptdate'])]

    s = orders[(orders['o_orderdate'] >= 19960101) & (orders['o_orderdate'] < 19960401) & r.exists()]
    s = s.groupby(['o_orderpriority']).aggr(order_count=('*', 'count'))

    db_driver.run(s)
