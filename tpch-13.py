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

if __name__ == '__main__':
    var1 = 'special'
    var2 = 'requests'

    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)

    part_o = orders[orders['o_comment'].str.not_contains(var1, var2)]

    r = customer.merge(part_o, on=(customer['c_custkey'] == orders['o_custkey']))

    c_orders = r.groupby(['c_custkey']).aggr(c_count=(r['o_orderkey'], 'count')).rename('c_orders')

    s = c_orders.groupby(['c_count']).aggr(custdist=('*', 'count'))

    pysdql.db_driver(db_path=r'T:/sdql').run(s, block=False)
    