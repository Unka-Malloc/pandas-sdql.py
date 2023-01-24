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

    var1 = 'UNITED KINGDOM'

    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)
    orders = pd.read_table(rf'{data_path}/orders.tbl', sep='|', index_col=False, header=None, names=pysdql.ORDERS_COLS)
    nation = pd.read_table(rf'{data_path}/nation.tbl', sep='|', index_col=False, header=None, names=pysdql.NATION_COLS)
    l1_cols = ['l1_orderkey', 'l1_partkey', 'l1_suppkey', 'l1_linenumber', 'l1_quantity', 'l1_extendedprice',
               'l1_discount', 'l1_tax', 'l1_returnflag', 'l1_linestatus', 'l1_shipdate', 'l1_commitdate',
               'l1_receiptdate', 'l1_shipinstruct', 'l1_shipmode', 'l1_comment']
    l1 = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=l1_cols)
    l1.__columns.field = 'l1'
    l2_cols = ['l2_orderkey', 'l2_partkey', 'l2_suppkey', 'l2_linenumber', 'l2_quantity', 'l2_extendedprice',
               'l2_discount', 'l2_tax', 'l2_returnflag', 'l2_linestatus', 'l2_shipdate', 'l2_commitdate',
               'l2_receiptdate', 'l2_shipinstruct', 'l2_shipmode', 'l2_comment']
    l2 = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=l2_cols)
    l2.__columns.field = 'l2'
    l3_cols = ['l3_orderkey', 'l3_partkey', 'l3_suppkey', 'l3_linenumber', 'l3_quantity', 'l3_extendedprice',
               'l3_discount', 'l3_tax', 'l3_returnflag', 'l3_linestatus', 'l3_shipdate', 'l3_commitdate',
               'l3_receiptdate', 'l3_shipinstruct', 'l3_shipmode', 'l3_comment']
    l3 = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=l3_cols)
    l3.__columns.field = 'l3'

    sub_n = nation[(nation['n_name'] == var1)]
    sub_n.__columns.field = 'sub_n'
    join_ns = supplier.merge(sub_n, left_on='s_nationkey', right_on='n_nationkey')
    join_ns.__columns.field = 'join_ns'
    sub_l1 = l1[(l1['l1_receiptdate'] > l1['l1_commitdate'])]
    sub_l1.__columns.field = 'sub_l1'
    r = join_ns.merge(sub_l1, left_on='s_suppkey', right_on='l1_suppkey')

    sub_o = orders[(orders['o_orderstatus'] == 'F')]
    sub_o.__columns.field = 'sub_o'
    r = r.merge(sub_o, left_on='l1_orderkey', right_on='o_orderkey')

    main_cols = pysdql.SUPPLIER_COLS + pysdql.ORDERS_COLS + pysdql.NATION_COLS + l1_cols

    r = r.merge(l2, left_on='l1_orderkey', right_on='l2_orderkey')
    r['exists_l2'] = np.select(
        [
            (r['l1_suppkey'] != r['l2_suppkey'])
        ],
        [
            1
        ],
        default=0)
    r = r.groupby(main_cols, as_index=False).agg(exists_l2=('exists_l2', 'sum'))
    r = r[r['exists_l2'] > 0]

    r = r.merge(l3, left_on='l1_orderkey', right_on='l3_orderkey')
    r['exists_l3'] = np.select(
        [
            (r['l1_suppkey'] != r['l3_suppkey'])
            & (r['l3_receiptdate'] > r['l3_commitdate'])
        ],
        [
            1
        ],
        default=0)
    r = r.groupby(main_cols, as_index=False).agg(exists_l3=('exists_l3', 'sum'))
    r = r[r['exists_l3'] == 0]

    r = r.groupby(['s_name'], as_index=False).agg(numwait=('l1_orderkey', 'count'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-21').run(r).export().to()
