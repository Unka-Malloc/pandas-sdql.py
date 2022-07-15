"""
select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = ':1'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= :4 and l_quantity <= :4 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = ':2'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= :5 and l_quantity <= :5 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = ':3'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= :6 and l_quantity <= :6 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
"""
import pysdql

if __name__ == '__main__':
    db_driver = pysdql.driver(db_path=r'T:/sdql')

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', header=pysdql.PART_COLS)

    c1 = (part['p_brand'] == ':1') \
         & (part['p_container'].isin(('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))) \
         & (lineitem['l_quantity'] >= ':4') \
         & (lineitem['l_quantity'] <= ':4 + 10') \
         & (part['p_size'] > 1) \
         & (part['p_size'] < 5) \
         & (lineitem['l_shipmode'].isin(('AIR', 'AIR REG'))) \
         & (lineitem['l_shipinstruct'] == 'DELIVER IN PERSON')
    c2 = (part['p_brand'] == ':2') \
         & (part['p_container'].isin(('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))) \
         & (lineitem['l_quantity'] >= ':5') \
         & (lineitem['l_quantity'] <= ':5 + 10') \
         & (part['p_size'] > 1) \
         & (part['p_size'] < 10) \
         & (lineitem['l_shipmode'].isin(('AIR', 'AIR REG'))) \
         & (lineitem['l_shipinstruct'] == 'DELIVER IN PERSON')
    c3 = (part['p_brand'] == ':3') \
         & (part['p_container'].isin(('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))) \
         & (lineitem['l_quantity'] >= ':6') \
         & (lineitem['l_quantity'] <= ':6 + 10') \
         & (part['p_size'] > 1) \
         & (part['p_size'] < 15) \
         & (lineitem['l_shipmode'].isin(('AIR', 'AIR REG'))) \
         & (lineitem['l_shipinstruct'] == 'DELIVER IN PERSON')

    r = pysdql.merge(lineitem, part, on=(part['p_partkey'] == lineitem['l_partkey']))

    r = r[c1 | c2 | c3]

    r = r.aggr(revenue=((lineitem['l_extendedprice'] * (1 - lineitem['l_discount'])), 'sum'))
