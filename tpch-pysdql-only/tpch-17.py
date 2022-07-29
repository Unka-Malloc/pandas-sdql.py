"""
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part,
	(SELECT l_partkey AS agg_partkey, 0.2 * avg(l_quantity) AS avg_quantity FROM lineitem GROUP BY l_partkey) part_agg
where
	p_partkey = l_partkey
	and agg_partkey = l_partkey
	and p_brand = ':1'
	and p_container = ':2'
	and l_quantity < avg_quantity
"""
import pysdql

if __name__ == '__main__':
    var1 = 'Brand#11'
    var2 = 'WRAP CASE'

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', names=pysdql.LINEITEM_COLS)
    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', names=pysdql.PART_COLS)

    # Co-related Nested Queries
    part_agg = lineitem.groupby(['l_partkey']) \
        .agg(agg_partkey=lineitem['l_partkey'], avg_quantity_1=(lineitem['l_quantity'], 'avg')) \
        .rename('part_agg')

    part_agg['avg_quantity'] = 0.2 * part_agg['avg_quantity_1']
    part_agg = part_agg[['agg_partkey', 'avg_quantity']]

    sub_p = part[(part['p_brand'] == var1) & (part['p_container'] == var2)].rename('sub_p')

    r = sub_p.merge(lineitem, on=sub_p['p_partkey'] == lineitem['l_partkey'])
    r = r.merge(part_agg, on=(r['l_partkey'] == part_agg['agg_partkey'])
                             & (r['l_quantity'] < part_agg['avg_quantity']))

    r = r.agg(avg_yearly=(r['l_extendedprice'] / 7.0, 'sum'))

    # r = r.agg(value=(r['l_extendedprice'], 'sum'))
    # r['avg_yearly'] = r['value'] / 7.0
    # r = r[['avg_yearly']]

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-17').run(r).export().to()
