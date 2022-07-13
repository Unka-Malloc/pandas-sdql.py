"""
select
	o_year,
	sum(case
		when nation = ':1' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = ':2'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = ':3'
	) as all_nations
group by
	o_year
"""
import pysdql

if __name__ == '__main__':
    part = pysdql.relation(name='part', cols=pysdql.PART_COLS)
    supplier = pysdql.relation(name='supplier', cols=pysdql.SUPPLIER_COLS)
    lineitem = pysdql.relation(name='lineitem', cols=pysdql.LINEITEM_COLS)
    orders = pysdql.relation(name='orders', cols=pysdql.ORDERS_COLS)
    customer = pysdql.relation(name='customer', cols=pysdql.CUSTOMER_COLS)
    n1 = pysdql.relation(name='nation', cols=pysdql.NATION_COLS)
    n2 = pysdql.relation(name='nation', cols=pysdql.NATION_COLS)
    region = pysdql.relation(name='region', cols=pysdql.REGION_COLS)

    r = pysdql.merge(part, supplier, lineitem, orders, customer, n1, n2, region,
                     on=((part['p_partkey'] == lineitem['l_partkey'])
                         & (supplier['s_suppkey'] == lineitem['l_suppkey'])
                         & (lineitem['l_orderkey'] == orders['o_orderkey'])
                         & (orders['o_custkey'] == customer['c_custkey'])
                         & (customer['c_nationkey'] == n1['c_nationkey'])
                         & (n1['n_regionkey'] == region['r_regionkey'])
                         & (supplier['s_nationkey'] == n2['n_nationkey'])
                         )
                     )[(region['r_name'] == ':2')
                       & (orders['o_orderdate'] > '1995-01-01')
                       & (orders['o_orderdate'] < '1996-12-31')
                       & (part['p_type'] == ':3')
                       ]
