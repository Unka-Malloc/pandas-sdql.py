"""
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> ':1'
	and p_type not like ':2%'
	and p_size in (:3, :4, :5, :6, :7, :8, :9, :10)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
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

    var1 = 'Brand#21'
    var2 = 'SMALL ANODIZED'
    var3 = (48, 33, 18, 16, 8, 3, 10, 42)

    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)
    part = pd.read_table(rf'{data_path}/part.tbl', sep='|', index_col=False, header=None, names=pysdql.PART_COLS)
    partsupp = pd.read_table(rf'{data_path}/partsupp.tbl', sep='|', index_col=False, header=None, names=pysdql.PARTSUPP_COLS)

    supplier['var1_index'] = supplier['s_comment'].str.find('Customer')
    supplier['var2_index'] = supplier['s_comment'].str.find('Complaints')

    sub_s = supplier[(supplier['var1_index'] != -1) & (supplier['var2_index'] != -1)
                     & (supplier['var1_index'] < supplier['var2_index'])]
    sub_s.columns.name = 'sub_s'

    sub_p = part[(part['p_brand'] != var1)
                 & (~part['p_type'].str.startswith(var2))
                 & (part['p_size'].isin(var3))]
    sub_p.columns.name = 'sub_p'

    r = sub_p.merge(partsupp, left_on='p_partkey', right_on='ps_partkey')

    r = r[~(r['ps_suppkey'].isin(sub_s['s_suppkey']))]

    r = r.drop_duplicates(['p_brand', 'p_type', 'p_size', 'ps_suppkey'])

    # COUNT DISTINCT
    r = r.groupby(['p_brand', 'p_type', 'p_size'], as_index=False).agg(supplier_cnt=('ps_suppkey', 'count'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path).run(r).export().to()
