"""
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = ':1'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * :2
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = ':1'
		)
"""
import pysdql

if __name__ == '__main__':
    var1 = 'PERU'
    var2 = 0.0001

    db_driver = pysdql.db_driver(db_path=r'T:/sdql')

    partsupp = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/partsupp.tbl', header=pysdql.PARTSUPP_COLS)
    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=pysdql.NATION_COLS)

    # agg_val = pysdql.merge(partsupp, supplier, nation,
    #                        on=(partsupp['ps_suppkey'] == supplier['s_suppkey'])
    #                           & (supplier['s_nationkey'] == nation['n_nationkey'])
    #                        )[(nation['n_name'] == ':1')] \
    #     .aggr({(partsupp['ps_supplycost'] * partsupp['ps_availqty'] * ':2'): 'sum'})[0]

    part_n = nation[(nation['n_name'] == var1)].rename('part_n')

    join_ns = part_n.merge(supplier, on=(part_n['n_nationkey'] == supplier['s_nationkey'])).rename('join_ns')

    r1 = join_ns.merge(partsupp, join_ns['s_suppkey'] == partsupp['ps_suppkey']).rename('r1')

    agg_val = (r1['ps_supplycost'] * r1['ps_availqty'] * 1).sum()

    # GOURPBY HAVING
    r = r1.groupby(['ps_partkey']).filter(lambda x: (x['ps_supplycost'] * x['ps_availqty']).sum() > agg_val)

    # SELECT GROUPBY AGGREGATION
    r = r.groupby(['ps_partkey']).aggr(value=(r['ps_supplycost'] * r['ps_availqty'], 'sum'))

    db_driver.run(r)
