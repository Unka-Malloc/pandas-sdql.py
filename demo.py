import time

import pysdql

if __name__ == '__main__':
    start_time = time.time()

    # optimized = pysdql.tpch_query(range(1, 23), verbose=False, optimize=True)

    # opt_part1 = pysdql.tpch_query(range(1, 11), verbose=True, optimize=True)

    # opt_part2 = pysdql.tpch_query(range(12, 23), verbose=True, optimize=True)

    # opt_single = pysdql.tpch_query(3, verbose=True, optimize=True)

    # unoptimized = pysdql.tpch_query(range(1, 23), verbose=True, optimize=False)

    # fix after refactor: 2, 15, 20

    # opt_part1 = pysdql.tpch_query(range(1, 11), verbose=True, optimize=False)

    # opt_part2 = pysdql.tpch_query(range(12, 23), verbose=True, optimize=False)

    # unopt_some = pysdql.tpch_query([7, 13, 20, 22], verbose=True, optimize=False)

    unopt_single = pysdql.tpch_query(2, verbose=True, optimize=False)

    # psql_verified = pysdql.tpch_query([1, 6, 12, 13, 14], verbose=True, optimize=True, mode='postgres')

    # psql = pysdql.tpch_query(3, verbose=True, optimize=True, mode='postgres')

    # psql_unopt = pysdql.tpch_query(9, verbose=True, optimize=False, mode='postgres')

    # psql_unopt_verified = pysdql.tpch_query([1, 3, 4, 5, 6, 10, 12, 13, 14, 16, 19], verbose=True, optimize=False, mode='postgres')

    # psql unopt current progress: 1, 3, 4, 5, 6, 10, 12, 13, 14, 16, 19

    # psql require copy feature: 2, 11,

    # psql unopt pass: [5, 6, 13, 16, 19]
    # psql pass: 1, 5,
    # psql pass remove projection: 1, 5, 10, 12, 16
    # psql failed: 3, 4, 6, 10, 12, 13, 14, 16, 19
    # psql error: 2, 7, 8, 9, 11, 15, 17, 18, 20, 21, 22

    # duck_unopt = pysdql.tpch_query(1, verbose=True, optimize=False, mode='duckdb')

    # duck_unopt_single = pysdql.tpch_query(16, verbose=True, optimize=False, mode='duckdb')

    # duck unopt pass: [1, 4, 5, 9, 13, 16]
    # duck unopt fail:
    # duck unopt error: ...

    end_time = time.time()

    print((end_time - start_time) / 60)

    '''
    j_df_1 = lineitem.merge(orders, left_on='l_orderkey', right_on='o_orderkey')
    j_df_1['revenue'] = joint_df.l_extendedprice * (1 - lineitem.l_discount)
    j_df_1.groupby(['l_orderkey', 'l_suppkey']).agg({'revenue': 'sum'})
    '''

    '''
    li_index = li.sum(lambda x: {x[0].l_orderkey: record({"l_suppkey": x[0].l_suppkey, 
                                                          "l_extendedprice": x[0].l_extendedprice, 
                                                          "l_discount": x[0].l_discount})})
    joint_df = ord.sum(lambda x: {record({"l_orderkey": x[0].o_orderkey, "l_suppkey": li_index[x[0].o_orderkey].l_suppkey}): 
                                            record({"revenue": li_index[x[0].o_orderkey].l_extendedprice 
                                                                    * (1 - li_index[x[0].o_orderkey].l_discount)})})
    '''
