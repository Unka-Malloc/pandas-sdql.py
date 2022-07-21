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
    var1 = 'MOROCCO'
    var2 = 'MIDDLE EAST'
    var3 = 'SMALL ANODIZED COPPER'

    db_driver = pysdql.db_driver(db_path=r'T:/sdql', name='tpch-8')

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

    sub_r = region[(region['r_name'] == var2)].rename('sub_r')
    sub_o = orders[(orders['o_orderdate'] >= '1995-01-01') & (orders['o_orderdate'] <= '1996-12-31')].rename('sub_o')

    r1 = n1.merge(sub_r, on=n1['n1_regionkey'] == sub_r['r_regionkey']).rename('r1')

    r2 = part.merge(lineitem, on=part['p_partkey'] == lineitem['l_partkey'])
    r2 = r2.merge(orders, on=r2['l_orderkey'] == orders['o_orderkey'])
    r2 = r2.merge(customer, on=r2['o_custkey'] == customer['c_custkey']).rename('r2')

    r = r1.merge(r2, on=r1['n1_nationkey'] == r2['c_nationkey'])
    r = r.merge(supplier, on=r['l_suppkey'] == supplier['s_suppkey'])
    r = r.merge(n2, on=r['s_nationkey'] == n2['n2_nationkey'])

    r[['o_year', 'volume', 'nation']] = [r['o_orderdate'].year,
                                         r['l_extendedprice'] * (1 - r['l_discount']),
                                         r['n2_name']]

    all_nations = r[['o_year', 'volume', 'nation']].rename('all_nations')

    all_nations['value1'] = all_nations.case(all_nations['nation'] == var1, all_nations['volume'], 0)

    s = all_nations.groupby(['o_year']).aggr(value2=(all_nations['value1'], 'sum'),
                                             value3=(all_nations['volume'], 'sum'))
    s['mkt_share'] = s['value2'] / s['value3']

    s = s[['o_year', 'mkt_share']]

    db_driver.run(s).export().to()
