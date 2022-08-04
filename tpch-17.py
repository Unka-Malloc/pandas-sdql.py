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
# Try replace pysdql with pandas to get result in pandas!
# import pandas as pd  # get answer in pandas
import pysdql as pd  # get answer in pysdql

# display all columns
pd.set_option('display.max_columns', None)
# display all rows
pd.set_option('display.max_rows', None)

if __name__ == '__main__':
    data_path = 'T:/UG4-Proj/datasets'
    sdql_database_path = r'T:/sdql'

    var1 = 'Brand#11'
    var2 = 'WRAP CASE'

    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    part = pd.read_table(rf'{data_path}/part.tbl', sep='|', index_col=False, header=None, names=pysdql.PART_COLS)

    # Co-related Nested Queries
    part_agg = lineitem.groupby(['l_partkey'], as_index=False).agg(tmp_val=('l_quantity', 'mean'))
    part_agg['avg_quantity'] = 0.2 * part_agg['tmp_val']
    part_agg['agg_partkey'] = part_agg['l_partkey']

    part_agg = part_agg[['avg_quantity', 'agg_partkey']]
    part_agg.columns.__name = 'part_agg'

    sub_p = part[(part['p_brand'] == var1) & (part['p_container'] == var2)]
    sub_p.columns.__name = 'sub_p'

    r = sub_p.merge(lineitem, left_on='p_partkey', right_on='l_partkey')

    r = r.merge(part_agg, left_on='l_partkey', right_on='agg_partkey')
    r = r[r['l_quantity'] < r['avg_quantity']]

    r['tmp_val'] = r['l_extendedprice'].sum()

    r['avg_yearly'] = r['tmp_val'] / 7.0

    r = r[['avg_yearly']].drop_duplicates()

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-17').run(r).export().to()
