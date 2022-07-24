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

if __name__ == '__main__':
    var1 = ('16', '12', '18', '14', '30', '27', '25')

    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)

    sub_c = customer[(customer['c_acctbal'] > 0.00)
                     & (customer['c_phone'].substring(0, 2).isin(var1))]

    avg_acctbal = sub_c.agg({sub_c['c_acctbal']: 'avg'}).rename('avg_acctbal')

    r = customer[customer['c_phone'].substring(0, 2).isin(var1)]
    r = r[r['c_acctbal'] > avg_acctbal]
    r = r[r['c_custkey'].not_exists(orders['o_custkey'])]

    r['cntrycode'] = customer['c_phone'].substring(0, 2)

    r = r[['cntrycode', 'c_acctbal']]

    custsale = r.rename('custsale')
    custsale = custsale.groupby(['cntrycode']).agg(numcust=('*', 'count'),
                                                   totacctbal=(custsale['c_acctbal'], 'sum'),)

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-22').run(custsale).export().to()