"""
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = ':1'
	and o_orderdate >= date ':2'
	and o_orderdate < date ':2' + interval '1' year
group by
	n_name
order by
	revenue desc
"""
import pysdql

if __name__ == '__main__':
    customer = pysdql.Relation(name='customer', cols=pysdql.CUSTOMER_COLS)
    orders = pysdql.Relation(name='orders', cols=pysdql.ORDERS_COLS)
    lineitem = pysdql.Relation(name='lineitem', cols=pysdql.LINEITEM_COLS)
    supplier = pysdql.Relation(name='supplier', cols=pysdql.SUPPLIER_COLS)
    nation = pysdql.Relation(name='nation', cols=pysdql.NATION_COLS)
    region = pysdql.Relation(name='region', cols=pysdql.REGION_COLS)

    r = pysdql.merge(customer, orders, lineitem, supplier, nation, region,
                     on=(lineitem['l_orderkey'] == orders['o_orderkey'])
                        & (lineitem['l_suppkey'] == supplier['s_suppkey'])
                        & (customer['c_nationkey'] == supplier['s_nationkey'])
                        & (supplier['s_nationkey'] == nation['n_nationkey'])
                        & (nation['n_regionkey'] == region['r_regionkey']))

    r = r[(region['r_name'] == ':1')
          & (orders['o_orderdate'] >= ':2')
          & (orders['o_orderdate'] >= ':2 + 1 year')]

    r = r.groupby(['n_name']).aggr(revenue=((lineitem['l_extendedprice'] * (1 - lineitem['l_discount'])), 'sum'))
