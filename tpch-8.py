"""
select
	o_year,
	sum(case
		when nation = ':1' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = ':2'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = ':3'
	) as all_nations
group by
	o_year
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

    var1 = 'MOROCCO'
    var2 = 'MIDDLE EAST'
    var3 = 'SMALL ANODIZED COPPER'

    part = pd.read_table(rf'{data_path}/part.tbl', sep='|', index_col=False, header=None, names=pysdql.PART_COLS)
    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)
    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    customer = pd.read_table(rf'{data_path}/customer.tbl', sep='|', index_col=False, header=None, names=pysdql.CUSTOMER_COLS)
    region = pd.read_table(rf'{data_path}/region.tbl', sep='|', index_col=False, header=None, names=pysdql.REGION_COLS)

    n1_cols = ['n1_nationkey', 'n1_name', 'n1_regionkey', 'n1_comment']
    n2_cols = ['n2_nationkey', 'n2_name', 'n2_regionkey', 'n2_comment']
    n1 = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=n1_cols)
    n2 = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=n2_cols)

    n1.columns.__name = 'n1'
    n2.columns.__name = 'n2'

    sub_r = region[(region['r_name'] == var2)]
    sub_r.columns.__name = 'sub_r'

    sub_o = orders[(orders['o_orderdate'] >= '1995-01-01') & (orders['o_orderdate'] <= '1996-12-31')]
    sub_o.columns.__name = 'sub_o'

    r1 = n1.merge(sub_r, left_on='n1_regionkey', right_on='r_regionkey')
    r1.columns.__name = 'r1'

    r2 = part.merge(lineitem, left_on='p_partkey', right_on='l_partkey')
    r2 = r2.merge(orders, left_on='l_orderkey', right_on='o_orderkey')
    r2 = r2.merge(customer, left_on='o_custkey', right_on='c_custkey')

    r2.columns.__name = 'r2'

    r = r1.merge(r2, left_on='n1_nationkey', right_on='c_nationkey')
    r = r.merge(supplier, left_on='l_suppkey', right_on='s_suppkey')
    r = r.merge(n2, left_on='s_nationkey', right_on='n2_nationkey')

    r['o_year'] = pd.DatetimeIndex(r['o_orderdate']).year
    r['volume'] = r['l_extendedprice'] * (1 - r['l_discount'])
    r['nation'] = r['n2_name']

    all_nations = r[['o_year', 'volume', 'nation']]

    all_nations.columns.__name = 'all_nations'

    all_nations['value1'] = np.select(
        [
            all_nations['nation'] == var1
        ],
        [
            all_nations['volume']
        ], default=0)

    s = all_nations.groupby(['o_year'], as_index=False).agg(value2=('value1', 'sum'),
                                                            value3=('volume', 'sum'))
    s['mkt_share'] = s['value2'] / s['value3']

    s = s[['o_year', 'mkt_share']]

    print(s)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-8').run(s).export().to()
