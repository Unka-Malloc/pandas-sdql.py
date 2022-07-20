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
    var1 = 'JORDAN'
    var2 = 'MIDDLE EAST'
    var3 = 'SMALL ANODIZED COPPER'

    db_driver = pysdql.db_driver(db_path=r'T:/sdql')

    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', header=pysdql.PART_COLS)
    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)
    region = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/region.tbl', header=pysdql.REGION_COLS)

    n1_cols = ['n1_nationkey', 'n1_name', 'n1_regionkey', 'n1_comment']
    n2_cols = ['n2_nationkey', 'n2_name', 'n2_regionkey', 'n2_comment']
    n1 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=n1_cols, name='n1')
    n2 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=n2_cols, name='n2')

    r1 = customer.merge(n1, on=customer['c_nationkey'] == n1['n1_nationkey'])
    r1 = r1.merge(region, on=r1['n1_regionkey'] == region['r_regionkey'])

    r1 = r1[(r1['r_name'] == var2)]

    r1 = r1.merge(orders, on=r1['c_custkey'] == orders['o_custkey'])

    r1 = r1[(r1['o_orderdate'] >= '1995-01-01') & (r1['o_orderdate'] <= '1996-12-31')]

    r1 = r1.merge(lineitem, on=r1['o_orderkey'] == lineitem['l_orderkey']).rename('r1')

    r2 = supplier.merge(n2, on=supplier['s_nationkey'] == n2['n2_nationkey']).rename('r2')

    r1 = r1.merge(r2, on=r1['l_suppkey'] == r2['s_suppkey'])

    r = r1.merge(part, on=r1['l_partkey'] == part['p_partkey'])

    r = r[(r['p_type'] == var3)]

    r['o_year'] = r['o_orderdate'].year
    r['volume'] = r['l_extendedprice'] * (1 - r['l_discount'])
    r['nation'] = r['n2_name']

    r = r[['o_year', 'volume', 'nation']]

    all_nations = r.rename('all_nations')

    all_nations['mkt_value'] = all_nations.case(all_nations['nation'] == var1, all_nations['volume'], 0)

    all_nations['mkt_value'] = all_nations['mkt_value'] / all_nations['volume']

    s = all_nations.groupby(['o_year']).aggr(mkt_share=(all_nations['mkt_value'], 'sum'))

    db_driver.run(s)
