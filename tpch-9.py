"""
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%:1%'
	) as profit
group by
	nation,
	o_year
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

    var1 = 'cornflower'

    part = pd.read_table(rf'{data_path}/part.tbl', sep='|', index_col=False, header=None, names=pysdql.PART_COLS)
    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)
    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    partsupp = pd.read_table(rf'{data_path}/partsupp.tbl', sep='|', index_col=False, header=None, names=pysdql.PARTSUPP_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    nation = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=pysdql.NATION_COLS)

    # part_p
    sub_p = part[part['p_name'].str.contains(var1)]
    sub_p.__columns.field = 'sub_p'

    # optimized hash join (part, partsupp)
    r1 = sub_p.merge(partsupp, left_on='p_partkey', right_on='ps_partkey')
    r1.__columns.field = 'r1'
    # optimized hash join (supplier, nation)
    r2 = supplier.merge(nation, left_on='s_nationkey', right_on='n_nationkey')
    # optimized hash join ((supplier, nation), (part, partsupp))
    r2 = r2.merge(r1, left_on='s_suppkey', right_on='ps_suppkey')
    r2.__columns.field = 'r2'
    # optimized hash join ((supplier, nation, part, partsupp), lineitem)
    r = r2.merge(lineitem, how='inner', left_on=['s_suppkey', 'ps_suppkey'], right_on=['l_suppkey', 'l_suppkey'])
    # optimized hash join ((supplier, nation, part, partsupp, lineitem), orders)
    r = r.merge(orders, left_on='l_orderkey', right_on='o_orderkey')

    r['nation'] = r['n_name']
    r['o_year'] = pd.DatetimeIndex(r['o_orderdate']).year
    r['amount'] = r['l_extendedprice'] * (1 - r['l_discount']) - r['ps_supplycost'] * r['l_quantity']

    profit = r[['nation', 'o_year', 'amount']]
    profit.__columns.field = 'profit'

    s = profit.groupby(['nation', 'o_year'], as_index=False).agg(sum_profit=('amount', 'sum'))

    print(s)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-9').run(s).export().to()



