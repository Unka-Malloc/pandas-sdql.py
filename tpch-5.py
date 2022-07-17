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
    db_driver = pysdql.driver(db_path=r'T:/sdql')

    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=pysdql.NATION_COLS)
    region = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/region.tbl', header=pysdql.REGION_COLS)

    r = pysdql.merge(customer, orders, lineitem, supplier, nation, region,
                     on=(customer['c_custkey'] == orders['o_custkey'])
                        & (lineitem['l_orderkey'] == orders['o_orderkey'])
                        & (lineitem['l_suppkey'] == supplier['s_suppkey'])
                        & (customer['c_nationkey'] == supplier['s_nationkey'])
                        & (supplier['s_nationkey'] == nation['n_nationkey'])
                        & (nation['n_regionkey'] == region['r_regionkey'])
                     )

    r = r[(region['r_name'] == 'AFRICA')
          & (orders['o_orderdate'] >= 19960101)
          & (orders['o_orderdate'] < 19970101)]

    r = r.groupby(['n_name']).aggr(revenue=((lineitem['l_extendedprice'] * (1 - lineitem['l_discount'])), 'sum'))

    db_driver.run(r, block=True)
