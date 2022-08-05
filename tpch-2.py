"""
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = :1
	and p_type like '%:2'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = ':3'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = ':3'
	)
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

    var1 = 14
    var2 = 'BRASS'
    var3 = 'EUROPE'

    part = pd.read_table(rf'{data_path}/part.tbl', sep='|', index_col=False, header=None, names=pysdql.PART_COLS)
    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)
    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    partsupp = pd.read_table(rf'{data_path}/partsupp.tbl', sep='|', index_col=False, header=None, names=pysdql.PARTSUPP_COLS)
    nation = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=pysdql.NATION_COLS)
    region = pd.read_table(rf'{data_path}/region.tbl', sep='|', index_col=False, header=None, names=pysdql.REGION_COLS)

    sub_r = region[region['r_name'] == var3]
    sub_r.columns.field = 'sub_r'

    r1 = partsupp.merge(supplier, left_on='ps_suppkey', right_on='s_suppkey')
    r1 = r1.merge(nation, left_on='s_nationkey', right_on='n_nationkey')
    r1 = r1.merge(sub_r, left_on='n_regionkey', right_on='r_regionkey')
    r1 = r1.groupby(['ps_partkey', 'ps_suppkey'], as_index=False).agg(min_supplycost=('ps_supplycost', 'min'))

    r1['min_partkey'] = r1['ps_partkey']
    r1['min_suppkey'] = r1['ps_suppkey']

    r1 = r1[['min_partkey', 'min_suppkey', 'min_supplycost']]

    r1.columns.field = 'r1'

    sub_p = part[part['p_size'] == var1]
    sub_p = sub_p[sub_p['p_type'].str.endswith(var2)]

    sub_p.columns.field = 'sub_p'

    r2 = nation.merge(sub_r, left_on='n_regionkey', right_on='r_regionkey')
    r2 = r2.merge(supplier, left_on='n_nationkey', right_on='s_nationkey')
    r2 = r2.merge(partsupp, left_on='s_suppkey', right_on='ps_suppkey')
    r2 = r2.merge(sub_p, left_on='ps_partkey', right_on='p_partkey')

    r2.columns.field = 'r2'

    r = r1.merge(r2,
                 left_on=['min_partkey', 'min_suppkey', 'min_supplycost'],
                 right_on=['ps_partkey', 'ps_suppkey', 'ps_supplycost'])

    r.columns.field = 'r'

    r = r[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-2').run(r).export().to()
