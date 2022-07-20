"""
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = ':1' and n2.n_name = ':2')
				or (n1.n_name = ':2' and n2.n_name = ':1')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
"""

import pysdql

if __name__ == '__main__':
    var1 = 'MOZAMBIQUE'
    var2 = 'JORDAN'

    db_driver = pysdql.db_driver(db_path=r'T:/sdql')

    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)

    n1_cols = ['n1_nationkey', 'n1_name', 'n1_regionkey', 'n1_comment']
    n2_cols = ['n2_nationkey', 'n2_name', 'n2_regionkey', 'n2_comment']
    n1 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=n1_cols, name='n1')
    n2 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=n2_cols, name='n2')

    r1 = supplier.merge(n1, on=(supplier['s_nationkey'] == n1['n1_nationkey'])).rename('r1')

    r2 = customer.merge(n2, on=(customer['c_nationkey'] == n2['n2_nationkey'])).rename('r2')

    r = r1.merge(r2, on=((r1['n1_name'] == 'MOZAMBIQUE') & (r2['n2_name'] == 'JORDAN'))
                        | ((r1['n1_name'] == 'JORDAN') & (r2['n2_name'] == 'MOZAMBIQUE')))

    r = r.merge(lineitem, on=(r['s_suppkey'] == lineitem['l_suppkey']))
    r = r.merge(orders, on=(r['l_orderkey'] == orders['o_orderkey']) & (r['c_custkey'] == orders['o_custkey']))

    r = r[(r['l_shipdate'] >= '1995-01-01') & (r['l_shipdate'] <= '1996-12-31')]

    r['supp_nation'] = r['n1_name']
    r['cust_nation'] = r['n2_name']
    r['volume'] = r['l_extendedprice'] * (1 - r['l_discount'])
    r['l_year'] = r['l_shipdate'].year

    r = r[['supp_nation', 'cust_nation', 'l_year', 'volume']].rename('shiping')

    r = r.groupby(['supp_nation', 'cust_nation', 'l_year']).aggr(revenue=(r['volume'], 'sum'))

    db_driver.run(r, block=False)
