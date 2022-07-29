"""
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = ':1' and n2.n_name = ':2')
				or (n1.n_name = ':2' and n2.n_name = ':1')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
"""
import pysdql
# Try replace pysdql with pandas to get result in pandas!
# import pandas as pd  # get answer in pandas
# import numpy as np  # for numpy.select(), must use together with pandas
import pysdql as pd  # get answer in pysdql
# import pysdqlnp as np  # for pysdqlnp.select(), must use together with pysdql

# display all columns
pd.set_option('display.max_columns', None)
# display all rows
pd.set_option('display.max_rows', None)

if __name__ == '__main__':
    data_path = 'T:/UG4-Proj/datasets'
    sdql_database_path = r'T:/sdql'

    var1 = 'PERU'
    var2 = 'MOROCCO'

    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)
    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    customer = pd.read_table(rf'{data_path}/customer.tbl', sep='|', index_col=False, header=None, names=pysdql.CUSTOMER_COLS)

    n1_cols = ['n1_nationkey', 'n1_name', 'n1_regionkey', 'n1_comment']
    n2_cols = ['n2_nationkey', 'n2_name', 'n2_regionkey', 'n2_comment']
    n1 = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=n1_cols)
    n2 = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=n2_cols)

    n1.columns.name = 'n1'
    n2.columns.name = 'n2'

    sub_n = n1.merge(n2, how='cross')
    r = sub_n[(sub_n['n1_name'] == var1) & (sub_n['n2_name'] == var2)
              | ((sub_n['n1_name'] == var2) & (sub_n['n2_name'] == var1))]

    r = r.merge(customer, left_on='n2_nationkey', right_on='c_nationkey')
    r = r.merge(supplier, left_on='n1_nationkey', right_on='s_nationkey')
    r = r.merge(orders, left_on='c_custkey', right_on='o_custkey')

    sub_l = lineitem[(lineitem['l_shipdate'] >= '1995-01-01') & (lineitem['l_shipdate'] <= '1996-12-31')]
    r = r.merge(sub_l, left_on=['o_orderkey', 's_suppkey'], right_on=['l_orderkey', 'l_suppkey'])

    r['supp_nation'] = r['n1_name']
    r['cust_nation'] = r['n2_name']
    r['volume'] = r['l_extendedprice'] * (1 - r['l_discount'])
    r['l_year'] = pd.DatetimeIndex(r['l_shipdate']).year

    r = r[['supp_nation', 'cust_nation', 'l_year', 'volume']]

    r.columns.name = 'shiping'

    r = r.groupby(['supp_nation', 'cust_nation', 'l_year'], as_index=False).agg(revenue=('volume', 'sum'))

    print(r)

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-7').run(r).export().to()

