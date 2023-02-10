import pysdql

from pysdql.core.dtypes.sdql_ir import *

from pysdql.query.tpch.template import *

if __name__ == '__main__':
    # lineitem = pysdql.DataFrame()
    #
    # tpch_q6(lineitem).to_sdqlir(False)

    test_some = pysdql.tpch_query(2, verbose=False, optimize=False)

    # print(pysdql.query.tpch.Qpandas.q21())

    # print(pysdql.query.tpch.Qsdql.q21())
