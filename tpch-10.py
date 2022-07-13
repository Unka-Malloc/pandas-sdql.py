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

if __name__ == '__main__':
    customer = pysdql.relation(name='customer', cols=pysdql.CUSTOMER_COLS)
    orders = pysdql.relation(name='orders', cols=pysdql.ORDERS_COLS)
    lineitem = pysdql.relation(name='lineitem', cols=pysdql.LINEITEM_COLS)
    nation = pysdql.relation(name='nation', cols=pysdql.NATION_COLS)

    r = pysdql.merge(customer, orders, lineitem, nation,
                     on=(customer['c_custkey'] == orders['o_custkey'])
                        & (lineitem['l_orderkey'] == orders['o_custkey'])
                        & (customer['c_nationkey'] == nation['n_nationkey'])
                     )

    r = r[(orders['o_orderdate'] >= ':1')
          & (orders['o_orderdate'] >= ':1 + 3 month')
          & (lineitem['l_returnflag'] == 'R')]

    r = r.groupby(['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address','c_comment']).aggr(revenue=((lineitem['l_extendedprice'] * (1 - lineitem['l_discount'])), 'sum'))