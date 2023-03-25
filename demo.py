import pysdql

if __name__ == '__main__':
    optimized = pysdql.tpch_query(range(1, 23), verbose=True, optimize=True)

    # opt_part1 = pysdql.tpch_query(range(1, 11), verbose=True, optimize=True)

    # opt_part2 = pysdql.tpch_query(range(12, 23), verbose=True, optimize=True)

    # opt_single = pysdql.tpch_query(13, verbose=True, optimize=True)

    # unoptimized = pysdql.tpch_query(range(1, 23), verbose=True, optimize=False)

    # opt_part1 = pysdql.tpch_query(range(1, 11), verbose=True, optimize=False)

    # opt_part2 = pysdql.tpch_query(range(12, 23), verbose=True, optimize=False)

    # unopt_some = pysdql.tpch_query([7, 13, 20, 22], verbose=True, optimize=False)

    # unopt_single = pysdql.tpch_query(20, verbose=True, optimize=False)

    # psql_verified = pysdql.tpch_query([1, 6, 12, 13], verbose=True, optimize=True, mode='postgres')

    # psql = pysdql.tpch_query(2, verbose=True, optimize=True, mode='postgres')

    # waiting_for_unique = pysdql.tpch_query([3, 4, 5, 10, 16, 19], verbose=True, optimize=True, mode='postgres')

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

