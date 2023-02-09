import pysdql

from pysdql.core.dtypes.sdql_ir import *

from pysdql.query.tpch.template import *

if __name__ == '__main__':
    test_one = pysdql.tpch_query(21)

    # print(pysdql.query.tpch.Qpandas.q21())

    # print(pysdql.query.tpch.Qsdql.q21())
