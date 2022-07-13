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
    lineitem = pysdql.relation(name='lineitem', cols=pysdql.LINEITEM_COLS)
    orders = pysdql.relation(name='orders', cols=pysdql.LINEITEM_COLS)

    r = pysdql.merge(lineitem, orders,
                     on=(lineitem['l_orderkey'] == orders['o_orderkey'])
                     )[lineitem['l_commitdate'] < lineitem['l_receiptdate']]

    orders = orders[(orders['o_orderdate'] >= ':1') & (orders['o_orderdate'] < ':1 + 3 month') & r.exists()]
    orders.groupby(['o_orderpriority']).aggr(order_count=('*', 'count'))
