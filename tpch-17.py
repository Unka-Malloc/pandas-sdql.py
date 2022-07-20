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
    db_driver = pysdql.db_driver(db_path=r'T:/sdql')

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', header=pysdql.PART_COLS)

    part_agg = lineitem.groupby(['l_partkey']) \
        .aggr(agg_partkey=lineitem['l_partkey'], avg_quantity=(0.2 * lineitem['l_quantity'], 'avg')) \
        .rename('part_agg')

    r = pysdql.merge(lineitem, part, part_agg,
                     on=((part['p_partkey'] == lineitem['l_partkey'])
                         & (part_agg['agg_partkey'] == lineitem['l_partkey']))
                     )

    r = r[(part['p_brand'] == 'Brand#13')
          & (part['p_container'] == 'JUMBO PKG')
          & (lineitem['l_quantity'] < part_agg['avg_quantity'])
          ]

    r = r.aggr(avg_yearly=((lineitem['l_extendedprice'] / 7.0), 'sum'))

    db_driver.run(r)
