import pysdql

if __name__ == '__main__':
    # results = pysdql.tpch_query(range(1, 23), verbose=False, optimize=True)

    # unoptimized = pysdql.tpch_query(3, verbose=False, optimize=True, mode='postgres')

    # unopt_errors = pysdql.tpch_query([11, 14, 15, 17, 18, 22], verbose=False, optimize=False)

    pysdql.tpch_query(17, verbose=False, optimize=True)

    # unopt_single = pysdql.tpch_query(14, verbose=True, optimize=False)

    # verified = pysdql.tpch_query([1, 6, 12, 13, 14], verbose=True, optimize=True, mode='postgres')

    # waiting_for_unique = pysdql.tpch_query([3, 4, 5, 10, 16, 19], verbose=True, optimize=True, mode='postgres')

    # result = pysdql.tpch_query(2, verbose=True, optimize=True, mode='postgres')

    # pysdql.tpch_query(14, verbose=True, optimize=True)

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

