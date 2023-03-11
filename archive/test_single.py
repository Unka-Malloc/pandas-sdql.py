import pysdql

if __name__ == '__main__':
    test_single = pysdql.tpch_query(1, verbose=True, optimize=True)

    # print(pysdql.query.tpch.Qpostgres.q6())

    # print(pysdql.query.tpch.Qsdql.q21())
