import pysdql

if __name__ == '__main__':
    test_some = pysdql.tpch_query(6, verbose=True, optimize=True, mode='postgres')

    # print(pysdql.query.tpch.Qpostgres.q6())

    # print(pysdql.query.tpch.Qsdql.q21())
