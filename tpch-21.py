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
    var1 = 'UNITED KINGDOM'

    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    l1_cols = ['l1_orderkey', 'l1_partkey', 'l1_suppkey', 'l1_linenumber', 'l1_quantity', 'l1_extendedprice',
               'l1_discount', 'l1_tax', 'l1_returnflag', 'l1_linestatus', 'l1_shipdate', 'l1_commitdate',
               'l1_receiptdate', 'l1_shipinstruct', 'l1_shipmode', 'l1_comment']
    l1 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=l1_cols, name='l1')
    l2_cols = ['l2_orderkey', 'l2_partkey', 'l2_suppkey', 'l2_linenumber', 'l2_quantity', 'l2_extendedprice',
               'l2_discount', 'l2_tax', 'l2_returnflag', 'l2_linestatus', 'l2_shipdate', 'l2_commitdate',
               'l2_receiptdate', 'l2_shipinstruct', 'l2_shipmode', 'l2_comment']
    l2 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=l2_cols, name='l2')
    l3_cols = ['l3_orderkey', 'l3_partkey', 'l3_suppkey', 'l3_linenumber', 'l3_quantity', 'l3_extendedprice',
               'l3_discount', 'l3_tax', 'l3_returnflag', 'l3_linestatus', 'l3_shipdate', 'l3_commitdate',
               'l3_receiptdate', 'l3_shipinstruct', 'l3_shipmode', 'l3_comment']
    l3 = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=l3_cols, name='l3')
    orders = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/orders.tbl', header=pysdql.ORDERS_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', header=pysdql.NATION_COLS)

    sub_n = nation[(nation['n_name'] == var1)].rename('sub_n')
    join_ns = supplier.merge(sub_n, on=supplier['s_nationkey'] == sub_n['n_nationkey']).rename('join_ns')
    sub_l1 = l1[(l1['l1_receiptdate'] > l1['l1_commitdate'])].rename('sub_l1')
    r = join_ns.merge(sub_l1, on=(join_ns['s_suppkey'] == sub_l1['l1_suppkey']))

    sub_o = orders[(orders['o_orderstatus'] == 'F')].rename('sub_o')
    r = r.merge(sub_o, on=(r['l1_orderkey'] == sub_o['o_orderkey']))

    sub_l3 = l3[(l3['l3_receiptdate'] > l3['l3_commitdate'])].rename('sub_l3')

    r = r[r['l1_orderkey'].exists(l2['l2_orderkey'], r['l1_suppkey'] != l2['l2_suppkey'])]
    r = r[r['l1_orderkey'].not_exists(sub_l3['l3_orderkey'], r['l1_suppkey'] != sub_l3['l3_suppkey'])]

    r = r.groupby(['s_name']).agg(numwait=('*', 'count'))

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-21').run(r).export().to()
