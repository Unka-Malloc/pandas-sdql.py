"""
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%:1%'
	) as profit
group by
	nation,
	o_year
"""
import pysdql

if __name__ == '__main__':
    var1 = 'cornflower'

    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', header=pysdql.PART_COLS)
    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    partsupp = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/partsupp.tbl', header=pysdql.PARTSUPP_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=pysdql.NATION_COLS)

    # part_p
    part_p = part[part['p_name'].startswith(var1)].rename('part_p')

    # hash join (part, partsupp)
    r1 = part_p.merge(partsupp, on=part_p['p_partkey'] == partsupp['ps_partkey']).rename('r1')

    # hash join (supplier, nation)
    r2 = supplier.merge(nation, on=(supplier['s_nationkey'] == nation['n_nationkey']))
    # hash join ((supplier, nation), (part, partsupp))
    r2 = r2.merge(r1, on=(r2['s_suppkey'] == r1['ps_suppkey'])).rename('r2')

    r = r2.merge(lineitem, on=(r2['s_suppkey'] == lineitem['l_suppkey']) & (r2['ps_suppkey'] == lineitem['l_suppkey']))
    r = r.merge(orders, on=(r['l_orderkey'] == orders['o_orderkey'])).rename('r')

    r['nation'] = r['n_name']
    r['o_year'] = r['o_orderdate'].year
    r['amount'] = r['l_extendedprice'] * (1 - r['l_discount']) - r['ps_supplycost'] * r['l_quantity']

    profit = r[['nation', 'o_year', 'amount']].rename('profit')

    s = profit.groupby(['nation', 'o_year']).aggr(sum_profit=(profit['amount'], 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql').run(r)



