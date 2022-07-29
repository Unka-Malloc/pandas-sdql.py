"""
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date ':1'
	and o_orderdate < date ':1' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
"""
import pysdql

if __name__ == '__main__':
    var1 = '1993-08-01'
    var2 = '1993-11-01'  # var1 + 3 month

    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', names=pysdql.CUSTOMER_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', names=pysdql.ORDERS_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', names=pysdql.LINEITEM_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', names=pysdql.NATION_COLS)

    part_o = orders[(orders['o_orderdate'] >= var1) & (orders['o_orderdate'] < var2)].rename('part_o')
    r = customer.merge(part_o, on=customer['c_custkey'] == part_o['o_custkey'])
    r = r.merge(nation, on=r['c_nationkey'] == nation['n_nationkey'])

    part_l = lineitem[lineitem['l_returnflag'] == 'R'].rename('part_l')
    r = r.merge(part_l, on=r['o_custkey'] == part_l['l_orderkey'])

    r = r.groupby(['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment']).agg(
        revenue=((r['l_extendedprice'] * (1 - r['l_discount'])), 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-10').run(r).export().to()
