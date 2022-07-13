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
    lineitem = pysdql.relation(name='lineitem', cols=pysdql.LINEITEM_COLS)
    part = pysdql.relation(name='part', cols=pysdql.PART_COLS)

    part_agg = lineitem.groupby(['l_partkey']) \
        .aggr(agg_partkey=lineitem['l_partkey'], avg_quantity=(0.2 * lineitem['l_quantity'], 'avg')) \
        .rename('part_agg')

    r = pysdql.merge(lineitem, part, part_agg,
                     on=((part['p_partkey'] == lineitem['l_partkey'])
                         & (part_agg['agg_partkey'] == lineitem['l_partkey']))
                     )
    r = r[(part['p_brand'] == ':1')
          & (part['p_container'] == ':2')
          & (lineitem['l_quantity'] < part_agg['avg_quantity'])]

    r = r.aggr(avg_yearly=((lineitem['l_extendedprice'] / 7.0), 'sum'))
