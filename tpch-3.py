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
    var1 = 'BUILDING'
    var2 = '1995-03-22'

    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)

    sub_c = customer[customer['c_mktsegment'] == var1].rename('sub_c')
    sub_o = orders[orders['o_orderdate'] < var2].rename('sub_o')
    sub_l = lineitem[lineitem['l_shipdate'] > var2].rename('sub_l')

    # 1M - 43s (without sub-table)
    # 1M - 28s (with sub-table)
    # r = sub_c.merge(sub_o, left_on='c_custkey', right_on='o_custkey')
    # r = r.merge(sub_l, left_on='o_orderkey', right_on='l_orderkey')

    # 1M - 73s (without sub-table)
    # 1M - 24s (with sub-table)
    r = sub_c.merge(sub_o, on=(sub_c['c_custkey'] == sub_o['o_custkey']))
    r = r.merge(sub_l, on=(r['o_orderkey'] == sub_l['l_orderkey']))

    r = r.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']) \
        .agg(revenue=((r['l_extendedprice'] * (1 - r['l_discount'])), 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-3').run(r).export().to()
