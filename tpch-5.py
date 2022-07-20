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
    var2 = '1993-01-01'
    var3 = '1994-01-01'  # var2 + 1 year

    db_driver = pysdql.db_driver(db_path=r'T:/sdql')

    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=pysdql.NATION_COLS)
    region = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/region.tbl', header=pysdql.REGION_COLS)

    r = customer.merge(right=orders, on=(customer['c_custkey'] == orders['o_custkey']))  # 1500 lines: 4s
    r = r.merge(lineitem, on=(r['o_orderkey'] == lineitem['l_orderkey']))  # 6005 lines: 59s
    r = r.merge(supplier, on=((r['l_suppkey'] == supplier['s_suppkey'])
                              & (r['c_nationkey'] == supplier['s_nationkey'])
                              )
                )  # 240 lines: 60s
    r = r.merge(nation, on=(r['s_nationkey'] == nation['n_nationkey']))  # 240 lines: 61s
    r = r.merge(region, on=(r['n_regionkey'] == region['r_regionkey']))  # 240 lines: 60s

    # r = pysdql.merge(customer, orders, lineitem, supplier, nation, region,
    #                  on=(customer['c_custkey'] == orders['o_custkey'])
    #                     & (orders['o_orderkey'] == lineitem['l_orderkey'])
    #                     & (lineitem['l_suppkey'] == supplier['s_suppkey'])
    #                     & (customer['c_nationkey'] == supplier['s_nationkey'])
    #                     & (supplier['s_nationkey'] == nation['n_nationkey'])
    #                     & (nation['n_regionkey'] == region['r_regionkey'])
    #                  )

    # r = customer.merge(right=orders, left_on='c_custkey', right_on='o_custkey')
    # r = r.merge(right=lineitem, left_on='o_orderkey', right_on='l_orderkey')
    # r = r.merge(right=supplier, left_on=['l_suppkey', 'c_nationkey'], right_on=['s_suppkey', 's_nationkey'])
    # r = r.merge(right=nation, left_on='s_nationkey', right_on='n_nationkey')
    # r = r.merge(right=region, left_on='n_regionkey', right_on='r_regionkey')

    r = r[(region['r_name'] == var1)
          & (orders['o_orderdate'] >= var2)
          & (orders['o_orderdate'] < var3)]

    r = r.groupby(['n_name']).aggr(revenue=((r['l_extendedprice'] * (1 - r['l_discount'])), 'sum'))

    db_driver.run(r, block=False)
