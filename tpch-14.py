"""
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date ':1'
	and l_shipdate < date ':1' + interval '1' month
"""
import pysdql

if __name__ == '__main__':
    var1 = '1995-03-01'
    var2 = '1995-04-01'  # var1 + 1 month

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', header=pysdql.PART_COLS)

    sub_l = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)].rename('sub_l')
    r = sub_l.merge(part, on=sub_l['l_partkey'] == part['p_partkey'])

    r['promo'] = r.case(r['p_type'].startswith('PROMO'), r['l_extendedprice'] * (1 - r['l_discount']), 0)

    r = r.aggr(promo_revenue=(100 * r['promo'] / (r['l_extendedprice'] * (1 - r['l_discount'])), 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql').run(r)
