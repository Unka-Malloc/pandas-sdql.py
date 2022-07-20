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

    db_driver = pysdql.db_driver(db_path=r'T:/sdql')

    customer = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/customer.tbl', header=pysdql.CUSTOMER_COLS)

    sub_c = customer[(customer['c_acctbal'] > 0.00)
                     & (customer['c_phone'].str.substring(0, 2).isin(var1))]

    db_driver.run(sub_c)