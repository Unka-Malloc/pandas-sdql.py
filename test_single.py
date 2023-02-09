import pysdql

from pysdql.core.dtypes.sdql_ir import *

from pysdql.query.tpch.template import *

if __name__ == '__main__':
    orders = pysdql.DataFrame()
    lineitem = pysdql.DataFrame()

    tpch_q12(orders, lineitem).opt_to_sdqlir()

    test_one = pysdql.tpch_query(12, verbose=False)

    # print(pysdql.query.tpch.Qpandas.q21())

    # print(pysdql.query.tpch.Qsdql.q21())
