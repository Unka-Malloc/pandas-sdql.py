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
    var1 = 'Brand#54'
    var2 = 'Brand#22'
    var3 = 'Brand#12'
    var4 = 1
    var4_1 = var4 + 10
    var5 = 18
    var5_1 = var5 + 10
    var6 = 26
    var6_1 = var6 + 10

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', header=pysdql.PART_COLS)

    a1 = (part['p_brand'] == var1) \
         & (part['p_container'].isin(('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))) \
         & (part['p_size'] >= 1) & (part['p_size'] <= 5)

    a2 = (part['p_brand'] == var2) \
         & (part['p_container'].isin(('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))) \
         & (part['p_size'] >= 1) & (part['p_size'] <= 10)

    a3 = (part['p_brand'] == 'Brand#13') \
         & (part['p_container'].isin(('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))) \
         & (part['p_size'] >= 1) & (part['p_size'] <= 15)

    sub_p = part[a1 | a2 | a3].rename('sub_p')

    b1 = (lineitem['l_quantity'] >= var4) & (lineitem['l_quantity'] <= var4_1) \
         & (lineitem['l_shipmode'].isin(('AIR', 'AIR REG'))) \
         & (lineitem['l_shipinstruct'] == 'DELIVER IN PERSON')

    b2 = (lineitem['l_quantity'] >= var5) & (lineitem['l_quantity'] <= var5_1) \
         & (lineitem['l_shipmode'].isin(('AIR', 'AIR REG'))) \
         & (lineitem['l_shipinstruct'] == 'DELIVER IN PERSON')

    b3 = (lineitem['l_quantity'] >= var6) \
         & (lineitem['l_quantity'] <= var6_1) \
         & (lineitem['l_shipmode'].isin(('AIR', 'AIR REG'))) \
         & (lineitem['l_shipinstruct'] == 'DELIVER IN PERSON')

    sub_l = lineitem[b1 | b2 | b3].rename('sub_l')

    r = sub_l.merge(sub_p, on=(sub_l['l_partkey'] == sub_p['p_partkey']))

    r = r.aggregate(revenue=((r['l_extendedprice'] * (1 - r['l_discount'])), 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-19').run(r).export().to()
