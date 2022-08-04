"""
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = ':1'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * :2
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = ':1'
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

    var1 = 'PERU'
    var2 = 0.0001

    partsupp = pd.read_table(rf'{data_path}/partsupp.tbl', sep='|', index_col=False, header=None, names=pysdql.PARTSUPP_COLS)
    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)
    nation = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=pysdql.NATION_COLS)

    sub_n = nation[(nation['n_name'] == var1)]
    sub_n.columns.__name = 'sub_n'

    r1 = sub_n.merge(supplier, left_on='n_nationkey', right_on='s_nationkey')
    r1.columns.__name = 'r1'

    r2 = r1.merge(partsupp, left_on='s_suppkey', right_on='ps_suppkey')
    r2.columns.__name = 'r2'

    agg_val = (r2['ps_supplycost'] * r2['ps_availqty'] * var2).sum()

    # GOURPBY HAVING
    r = r2.groupby(['ps_partkey']).filter(lambda x: (x['ps_supplycost'] * x['ps_availqty']).sum() > agg_val)

    r['value'] = r['ps_supplycost'] * r['ps_availqty']

    # SELECT GROUPBY AGGREGATION
    r = r.groupby(['ps_partkey'], as_index=False).agg(value=('value', 'sum'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-11').run(r).export().to()
