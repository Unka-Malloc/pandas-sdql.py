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
    var1 = 'PERU'
    var2 = 'MOROCCO'

    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', names=pysdql.SUPPLIER_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', names=pysdql.LINEITEM_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', names=pysdql.ORDERS_COLS)
    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', names=pysdql.CUSTOMER_COLS)

    n1_cols = ['n1_nationkey', 'n1_name', 'n1_regionkey', 'n1_comment']
    n2_cols = ['n2_nationkey', 'n2_name', 'n2_regionkey', 'n2_comment']
    n1 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', names=n1_cols, r_name='n1')
    n2 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', names=n2_cols, r_name='n2')

    r = n1.merge(n2, on=((n1['n1_name'] == var1) & (n2['n2_name'] == var2))
                        | ((n1['n1_name'] == var2) & (n2['n2_name'] == var1)))

    r = r.merge(customer, on=r['n2_nationkey'] == customer['c_nationkey'])
    r = r.merge(supplier, on=r['n1_nationkey'] == supplier['s_nationkey'])
    r = r.merge(orders, on=r['c_custkey'] == orders['o_custkey'])

    sub_l = lineitem[(lineitem['l_shipdate'] >= '1995-01-01') & (lineitem['l_shipdate'] <= '1996-12-31')].rename('sub_l')

    r = r.merge(sub_l, on=(r['o_orderkey'] == sub_l['l_orderkey']) & (r['s_suppkey'] == sub_l['l_suppkey']))

    r[['supp_nation', 'cust_nation', 'volume', 'l_year']] = [r['n1_name'],
                                                             r['n2_name'],
                                                             r['l_extendedprice'] * (1 - r['l_discount']),
                                                             r['l_shipdate'].year]

    r = r[['supp_nation', 'cust_nation', 'l_year', 'volume']].rename('shiping')

    r = r.groupby(['supp_nation', 'cust_nation', 'l_year']).agg(revenue=(r['volume'], 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-7').run(r).export().to()
