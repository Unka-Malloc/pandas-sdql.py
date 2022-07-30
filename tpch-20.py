"""
select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp,
			(
				select
					l_partkey agg_partkey,
					l_suppkey agg_suppkey,
					0.5 * sum(l_quantity) AS agg_quantity
				from
					lineitem
				where
					l_shipdate >= date ':2'
					and l_shipdate < date ':2' + interval '1' year
				group by
					l_partkey,
					l_suppkey
			) agg_lineitem
		where
			agg_partkey = ps_partkey
			and agg_suppkey = ps_suppkey
			and ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like ':1%'
			)
			and ps_availqty > agg_quantity
	)
	and s_nationkey = n_nationkey
	and n_name = ':3'
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

    var1 = 'orange'
    var2 = '1995-01-01'
    var2_1 = '1996-01-01'  # var2 + 1 year
    var3 = 'UNITED STATES'

    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    part = pd.read_table(rf'{data_path}/part.tbl', sep='|', index_col=False, header=None, names=pysdql.PART_COLS)
    partsupp = pd.read_table(rf'{data_path}/partsupp.tbl', sep='|', index_col=False, header=None, names=pysdql.PARTSUPP_COLS)
    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)
    nation = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=pysdql.NATION_COLS)

    sub_l = lineitem[(lineitem['l_shipdate'] >= var2) & (lineitem['l_shipdate'] < var2_1)]
    sub_l.columns.name = 'sub_l'

    agg_lineitem = sub_l.groupby(['l_partkey', 'l_suppkey'], as_index=False).agg(tmp_val=('l_quantity', 'sum'))

    agg_lineitem['agg_partkey'] = agg_lineitem['l_partkey']
    agg_lineitem['agg_suppkey'] = agg_lineitem['l_suppkey']
    agg_lineitem['agg_quantity'] = agg_lineitem['tmp_val'] * 0.5

    agg_lineitem.columns.name = 'agg_lineitem'

    r2 = partsupp.merge(agg_lineitem, left_on=['ps_partkey', 'ps_suppkey'], right_on=['agg_partkey', 'agg_suppkey'])
    r2 = r2[r2['ps_availqty'] > r2['agg_quantity']]

    sub_p = part[part['p_name'].str.startswith(var1)]
    sub_p.columns.name = 'sub_p'

    r1 = r2[(r2['ps_partkey'].isin(sub_p['p_partkey']))]
    r1.columns.name = 'r1'

    sub_n = nation[(nation['n_name'] == var3)]
    sub_n.columns.name = 'sub_n'

    s = supplier.merge(sub_n, left_on='s_nationkey', right_on='n_nationkey')

    s = s[(s['s_suppkey'].isin(r1['ps_suppkey']))]

    s = s[['s_name', 's_address']]

    print(s)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-20').run(s).export().to()
