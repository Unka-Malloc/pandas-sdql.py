"""
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in (':1', ':2')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date ':3'
	and l_receiptdate < date ':3' + interval '1' year
group by
	l_shipmode
"""
import pysdql

if __name__ == '__main__':
    var1 = 'FOB'
    var2 = 'AIR'
    var3 = '1995-01-01'
    var4 = '1996-01-01'

    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)

    sub_l = lineitem[(lineitem['l_shipmode'].isin((var1, var2)))
                     & (lineitem['l_commitdate'] < lineitem['l_receiptdate'])
                     & (lineitem['l_shipdate'] < lineitem['l_commitdate'])
                     & (lineitem['l_receiptdate'] >= var3) & (lineitem['l_receiptdate'] < var4)].rename('sub_l')

    r = orders.merge(sub_l, on=(orders['o_orderkey'] == sub_l['l_orderkey']))

    r['high_line_priority'] = r.case((r['o_orderpriority'] == '1-URGENT') | (r['o_orderpriority'] == '2-HIGH'), 1, 0)

    r['low_line_priority'] = r.case((r['o_orderpriority'] != '1-URGENT') | (r['o_orderpriority'] != '2-HIGH'), 1, 0)

    r = r.groupby(['l_shipmode']).aggregate(high_line_count=(r['high_line_priority'], 'sum'),
                                            low_line_count=(r['low_line_priority'], 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-12').run(r).export().to()
