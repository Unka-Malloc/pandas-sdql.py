"""
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select
            *
        from
            lineitem l2
        where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select
            *
        from
            lineitem l3
        where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey
    and n_name = ':1'
group by
    s_name
"""
import pysdql

if __name__ == '__main__':
    db_driver = pysdql.driver(db_path=r'T:/sdql')

    supplier = pysdql.relation(name='supplier', cols=pysdql.SUPPLIER_COLS)
    lineitem = pysdql.relation(name='lineitem', cols=pysdql.LINEITEM_COLS)
    orders = pysdql.relation(name='orders', cols=pysdql.ORDERS_COLS)
    nation = pysdql.relation(name='nation', cols=pysdql.NATION_COLS)

    r1 = lineitem[(lineitem['l_orderkey'] == lineitem['l_orderkey'])
                  & (lineitem['l_suppkey'] != lineitem['l_suppkey'])].rename('r1')

    r2 = lineitem[(lineitem['l_orderkey'] == lineitem['l_orderkey'])
                  & (lineitem['l_suppkey'] != lineitem['l_suppkey'])
                  & (lineitem['l_receiptdate'] > lineitem['l_commitdate'])].rename('r2')

    s = pysdql.merge(supplier, lineitem, orders, nation,
                     on=(supplier['s_suppkey'] == lineitem['l_suppkey'])
                        & (orders['o_orderkey'] == lineitem['l_orderkey'])
                        & (supplier['s_nationkey'] == nation['n_nationkey'])
                     )[(orders['o_orderstatus'] == 'F')
                       & (lineitem['l_receiptdate'] > lineitem['l_commitdate'])
                       & (nation['n_name'] == ':1')
                       & r1.exists()
                       & r2.not_exists()]

    s = s.groupby(['s_name']).aggr(numwait=('*', 'count'))

    s.get_result()
