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
    db_driver = pysdql.driver(db_path=r'T:/sdql')

    orders = pysdql.relation(name='orders', cols=pysdql.ORDERS_COLS)
    lineitem = pysdql.relation(name='lineitem', cols=pysdql.LINEITEM_COLS)

    r = pysdql.merge(orders, lineitem, on=(orders['o_orderkey'] == lineitem['l_orderkey']))

    r = r[(lineitem['l_shipmode'].isin((':1', ':2')))
          & (lineitem['l_commitdate'] < lineitem['l_receiptdate'])
          & (lineitem['l_shipdate'] < lineitem['l_commitdate'])
          & (lineitem['l_receiptdate'] >= ':3')
          & (lineitem['l_receiptdate'] < ':3 + 1 year')
          ]

    r['high_line_priority'] = r.case((r['o_orderpriority'] == '1-URGENT') | (r['o_orderpriority'] == '2-HIGH'), 1, 0)

    r['low_line_priority'] = r.case((r['o_orderpriority'] != '1-URGENT') | (r['o_orderpriority'] != '2-HIGH'), 1, 0)

    r = r.groupby(['l_shipmode']).aggr(high_line_count=(r['high_line_priority'], 'sum'),
                                       low_line_count=(r['low_line_priority'], 'sum'))


