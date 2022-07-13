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
    partsupp = pysdql.relation(name='partsupp', cols=pysdql.PARTSUPP_COLS)
    supplier = pysdql.relation(name='supplier', cols=pysdql.SUPPLIER_COLS)
    nation = pysdql.relation(name='nation', cols=pysdql.NATION_COLS)

    # agg_val = pysdql.merge(partsupp, supplier, nation,
    #                        on=(partsupp['ps_suppkey'] == supplier['s_suppkey'])
    #                           & (supplier['s_nationkey'] == nation['n_nationkey'])
    #                        )[(nation['n_name'] == ':1')] \
    #     .aggr({(partsupp['ps_supplycost'] * partsupp['ps_availqty'] * ':2'): 'sum'})[0]

    s = pysdql.merge(partsupp, supplier, nation,
                     on=(partsupp['ps_suppkey'] == supplier['s_suppkey'])
                        & (supplier['s_nationkey'] == nation['n_nationkey']),
                     name='S'
                     )[(nation['n_name'] == ':1')]
    agg_val = (s['ps_supplycost'] * s['ps_availqty'] * ':2').sum()

    # FROM
    r = pysdql.merge(partsupp, supplier, nation,
                     on=(partsupp['ps_suppkey'] == supplier['s_suppkey'])
                        & (supplier['s_nationkey'] == nation['n_nationkey']),
                     name='R'
                     )
    # WHERE
    r = r[(nation['n_name'] == ':1')]

    # GOURPBY HAVING
    r = r.groupby(['ps_partkey']).filter(lambda x: (x['ps_supplycost'] * x['ps_availqty']).sum() > agg_val)

    # SELECT GROUPBY AGGREGATION
    r = r.groupby(['ps_partkey']).aggr(value=(r['val'], 'sum'))
