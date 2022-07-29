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
    var1 = 251

    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', names=pysdql.CUSTOMER_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', names=pysdql.ORDERS_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', names=pysdql.LINEITEM_COLS)

    aggr_l = lineitem.groupby(['l_orderkey']).filter(lambda x: x['l_quantity'].sum() > var1)

    sub_o = orders[(orders['o_orderkey'].isin(aggr_l['l_orderkey']))]

    r = customer.merge(sub_o, on=(customer['c_custkey'] == sub_o['o_custkey']))
    r = r.merge(lineitem, on=r['o_orderkey'] == lineitem['l_orderkey'])

    r = r.groupby(['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'])\
        .aggregate({r['l_quantity']: 'sum'})

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-18').run(r).export().to()
