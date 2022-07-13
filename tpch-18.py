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

if __name__ == '__main__':
    customer = pysdql.relation(name='customer', cols=pysdql.CUSTOMER_COLS)
    orders = pysdql.relation(name='orders', cols=pysdql.ORDERS_COLS)
    lineitem = pysdql.relation(name='lineitem', cols=pysdql.LINEITEM_COLS)

    r = lineitem.groupby(['l_orderkey']).filter(lambda x: x['l_quantity'].sum() > ':1')[['l_orderkey']]

    s = pysdql.merge(customer, orders, lineitem,
                     on=(customer['c_custkey'] == orders['o_custkey'])
                        & (orders['o_orderkey'] == lineitem['l_orderkey']),
                     name='S'
                     )
    s = s[(s['o_orderkey'].isin(r['l_orderkey']))]
    s = s.groupby(['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'])\
        .aggr({s['l_quantity']: 'sum'})
    s.show()
