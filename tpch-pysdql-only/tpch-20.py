"""
select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp,
			(
				select
					l_partkey agg_partkey,
					l_suppkey agg_suppkey,
					0.5 * sum(l_quantity) AS agg_quantity
				from
					lineitem
				where
					l_shipdate >= date ':2'
					and l_shipdate < date ':2' + interval '1' year
				group by
					l_partkey,
					l_suppkey
			) agg_lineitem
		where
			agg_partkey = ps_partkey
			and agg_suppkey = ps_suppkey
			and ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like ':1%'
			)
			and ps_availqty > agg_quantity
	)
	and s_nationkey = n_nationkey
	and n_name = ':3'
"""
import pysdql

if __name__ == '__main__':
    var1 = 'orange'
    var2 = '1995-01-01'
    var2_1 = '1996-01-01'  # var2 + 1 year
    var3 = 'UNITED STATES'

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', names=pysdql.LINEITEM_COLS)
    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', names=pysdql.PART_COLS)
    partsupp = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/partsupp.tbl', names=pysdql.PARTSUPP_COLS)
    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', names=pysdql.SUPPLIER_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', names=pysdql.NATION_COLS)

    sub_l = lineitem[(lineitem['l_shipdate'] >= var2) & (lineitem['l_shipdate'] < var2_1)].rename('sub_l')
    agg_lineitem = sub_l.groupby(['l_partkey', 'l_suppkey']) \
        .aggregate(agg_partkey=sub_l['l_partkey'],
                   agg_suppkey=sub_l['l_suppkey'],
                   agg_quantity=(0.5 * sub_l['l_quantity'], 'sum')) \
        .rename('agg_lineitem')

    r = partsupp.merge(agg_lineitem, on=(partsupp['ps_partkey'] == agg_lineitem['agg_partkey'])
                                        & (partsupp['ps_suppkey'] == agg_lineitem['agg_suppkey'])
                                        & (partsupp['ps_availqty'] > agg_lineitem['agg_quantity']))

    sub_p = part[part['p_name'].startswith(var1)].rename('sub_p')

    r = r[(r['ps_partkey'].isin(sub_p['p_partkey']))]

    sub_n = nation[(nation['n_name'] == var3)].rename('sub_n')

    s = supplier.merge(sub_n, on=supplier['s_nationkey'] == sub_n['n_nationkey'])

    s = s[(s['s_suppkey'].isin(r['ps_suppkey']))]

    s = s[['s_name', 's_address']]

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-20').run(s).export().to()
