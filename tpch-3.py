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

if __name__ == '__main__':
    db_driver = pysdql.driver(db_path=r'T:/sdql')

    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)

    r = pysdql.merge(customer, orders, lineitem,
                     on=((customer['c_custkey'] == orders['o_custkey'])
                         & (orders['o_orderkey'] == lineitem['l_orderkey'])))

    r = r[(customer['c_mktsegment'] == 'BUILDING') & (orders['o_orderdate'] < 19960301) & (lineitem['l_shipdate'] > 19960301)]

    r = r.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']) \
        .aggr(revenue=((lineitem['l_extendedprice'] * (1 - lineitem['l_discount'])), 'sum'))

    db_driver.run(r)
