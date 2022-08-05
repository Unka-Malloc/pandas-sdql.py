"""
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				(':1', ':2', ':3', ':4', ':5', ':6', ':7')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						(':1', ':2', ':3', ':4', ':5', ':6', ':7')
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
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

    var1 = ('16', '12', '18', '14', '30', '27', '25')

    customer = pd.read_table(rf'{data_path}/customer.tbl', sep='|', index_col=False, header=None, names=pysdql.CUSTOMER_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)

    sub_c = customer[(customer['c_acctbal'] > 0.00)
                     & (customer['c_phone'].str.slice(0, 2).isin(var1))]
    sub_c.columns.field = 'sub_c'

    avg_acctbal = sub_c['c_acctbal'].mean()

    r = customer[customer['c_phone'].str.slice(0, 2).isin(var1)]

    r = r[r['c_acctbal'] > avg_acctbal]

    r = r.merge(orders, how='cross')
    r['exists'] = np.select(
        [
            (r['o_custkey'] == r['c_custkey'])
        ],
        [
            1
        ],
        default=0)
    r = r.groupby(pysdql.CUSTOMER_COLS, as_index=False).agg(exists=('exists', 'sum'))
    r = r[r['exists'] == 0]

    r['cntrycode'] = r['c_phone'].str.slice(0, 2)

    custsale = r[['cntrycode', 'c_acctbal']]
    custsale.columns.field = 'custsale'

    custsale = custsale.groupby(['cntrycode'], as_index=False).agg(numcust=('c_acctbal', 'count'), totacctbal=('c_acctbal', 'sum'))

    print(custsale)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-22').run(custsale).export().to()
