"""
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date ':1'
	and o_orderdate < date ':1' + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
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

    var1 = '1993-08-01'
    var2 = '1993-11-01'  # var1 + 3 month

    customer = pd.read_table(rf'{data_path}/customer.tbl', sep='|', index_col=False, header=None, names=pysdql.CUSTOMER_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    nation = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=pysdql.NATION_COLS)

    sub_o = orders[(orders['o_orderdate'] >= var1) & (orders['o_orderdate'] < var2)]
    sub_o.columns.__name = 'sub_o'

    r = customer.merge(sub_o, left_on='c_custkey', right_on='o_custkey')
    r = r.merge(nation, left_on='c_nationkey', right_on='n_nationkey')

    sub_l = lineitem[lineitem['l_returnflag'] == 'R']
    sub_l.columns.__name = 'sub_l'
    r = r.merge(sub_l, left_on='o_custkey', right_on='l_orderkey')

    r['value'] = r['l_extendedprice'] * (1 - r['l_discount'])

    r = r.groupby(['c_custkey', 'c_name', 'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment'], as_index=False) \
        .agg(revenue=('value', 'sum'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-10').run(r).export().to()
