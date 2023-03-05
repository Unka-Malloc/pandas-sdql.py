import pysdql

if __name__ == '__main__':
    natural_pass = [1, 6, 12]
    regex_pass = []

    test_some = pysdql.tpch_query(natural_pass + regex_pass, verbose=True, optimize=True, mode='postgres')

    # print(pysdql.query.tpch.Qpostgres.q6())

    # print(pysdql.query.tpch.Qsdql.q21())
