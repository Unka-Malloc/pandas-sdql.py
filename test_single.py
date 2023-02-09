import pysdql

from pysdql.core.dtypes.sdql_ir import *

from pysdql.query.tpch.template import *

if __name__ == '__main__':
    # partsupp = pysdql.DataFrame()
    # supplier = pysdql.DataFrame()
    # nation = pysdql.DataFrame()
    #
    # tpch_q11(partsupp, supplier, nation).opt_to_sdqlir()

    test_one = pysdql.tpch_query(22, verbose=False)

    # print(pysdql.query.tpch.Qpandas.q21())

    # print(pysdql.query.tpch.Qsdql.q21())
