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

    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    l1 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    l2 = l1.rename('l2')
    l3 = l1.rename('l3')
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=pysdql.NATION_COLS)

    r1 = pysdql.merge(l1, l2,
                      on=((l2['l_orderkey'] == l1['l_orderkey'])
                          & (l2['l_suppkey'] != l1['l_suppkey']))
                      ).rename('r1')

    r2 = pysdql.merge(l1, l3,
                      on=((l3['l_orderkey'] == l1['l_orderkey'])
                          & (l3['l_suppkey'] != l1['l_suppkey'])
                          & (l3['l_receiptdate'] > l3['l_commitdate']))
                      ).rename('r2')

    s = pysdql.merge(supplier, l1, orders, nation,
                     on=(supplier['s_suppkey'] == l1['l_suppkey'])
                        & (orders['o_orderkey'] == l1['l_orderkey'])
                        & (supplier['s_nationkey'] == nation['n_nationkey'])
                     )

    s = s[(orders['o_orderstatus'] == 'F')
          # & (l1['l_receiptdate'] > l1['l_commitdate'])
          # & (nation['n_name'] == 'MOROCCO')
          # & r1.exists()
          # & r2.not_exists()
          ]

    s = s.groupby(['s_name']).aggr(numwait=('*', 'count'))

    db_driver.run(s, block=True)
