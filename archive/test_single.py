import pysdql

if __name__ == '__main__':
    test_some = pysdql.tpch_query(range(1, 23), verbose=False, optimize=True)

    # print(pysdql.query.tpch.Qpandas.q21())

    # print(pysdql.query.tpch.Qsdql.q21())
