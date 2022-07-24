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
    var1 = 'MIDDLE EAST'
    var2 = '1995-01-01'
    var3 = '1996-01-01'  # var2 + 1 year

    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=pysdql.NATION_COLS)
    region = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/region.tbl', header=pysdql.REGION_COLS)

    sub_r = region[region['r_name'] == var1].rename('sub_r')
    sub_o = orders[(orders['o_orderdate'] >= var2) & (orders['o_orderdate'] < var3)].rename('sub_o')

    # 1M - 3s
    r = sub_r.merge(nation, on=(sub_r['r_regionkey'] == nation['n_regionkey']))
    r = r.merge(customer, on=(r['n_nationkey'] == customer['c_nationkey']))
    r = r.merge(sub_o, on=(r['c_custkey'] == sub_o['o_custkey']))
    r = r.merge(lineitem, on=(r['o_orderkey'] == lineitem['l_orderkey']))
    r = r.merge(supplier, on=(r['c_nationkey'] == supplier['s_nationkey'])
                             & (r['n_nationkey'] == supplier['s_nationkey'])
                             & (r['l_suppkey'] == supplier['s_suppkey']))

    # 1M - 18s
    # r = sub_r.merge(right=nation, left_on='r_regionkey', right_on='n_regionkey')
    # r = r.merge(right=customer, left_on='n_nationkey', right_on='c_nationkey')
    # r = r.merge(right=sub_o, left_on='c_custkey', right_on='o_custkey')
    # r = r.merge(right=lineitem, left_on='o_orderkey', right_on='l_orderkey')
    # r = r.merge(right=supplier, on=(r['c_nationkey'] == supplier['s_nationkey'])
    #                                & (r['n_nationkey'] == supplier['s_nationkey'])
    #                                & (r['l_suppkey'] == supplier['s_suppkey']))

    r = r.groupby(['n_name']).agg(revenue=((r['l_extendedprice'] * (1 - r['l_discount'])), 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-5').run(r).export().to()
