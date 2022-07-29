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
# Try replace pysdql with pandas to get result in pandas!
# import pandas as pd  # get answer in pandas
# import numpy as np  # for numpy.select(), must use together with pandas
import pysdql as pd  # get answer in pysdql
import pysdqlnp as np  # for pysdqlnp.select(), must use together with pysdql

# display all columns
pd.set_option('display.max_columns', None)
# display all rows
pd.set_option('display.max_rows', None)

if __name__ == '__main__':
    data_path = 'T:/UG4-Proj/datasets'
    sdql_database_path = r'T:/sdql'

    var1 = 'FOB'
    var2 = 'AIR'
    var3 = '1995-01-01'
    var4 = '1996-01-01'

    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)

    sub_l = lineitem[(lineitem['l_shipmode'].isin((var1, var2)))
                     & (lineitem['l_commitdate'] < lineitem['l_receiptdate'])
                     & (lineitem['l_shipdate'] < lineitem['l_commitdate'])
                     & (lineitem['l_receiptdate'] >= var3) & (lineitem['l_receiptdate'] < var4)]
    sub_l.columns.name = 'sub_l'

    r = orders.merge(sub_l, left_on='o_orderkey', right_on='l_orderkey')

    r['high_line_priority'] = np.select(
        [
            (r['o_orderpriority'] == '1-URGENT') | (r['o_orderpriority'] == '2-HIGH')
        ],
        [
            1
        ],
        default=0)

    r['low_line_priority'] = np.select(
        [
            (r['o_orderpriority'] != '1-URGENT') | (r['o_orderpriority'] != '2-HIGH')
        ],
        [
            1
        ],
        default=0)

    r = r.groupby(['l_shipmode'], as_index=False)\
        .agg(high_line_count=('high_line_priority', 'sum'),
             low_line_count=('low_line_priority', 'sum'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-12').run(r).export().to()
