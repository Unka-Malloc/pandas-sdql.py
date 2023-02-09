import pysdql

from pysdql.core.dtypes.sdql_ir import *

from pysdql.query.tpch.template import *

if __name__ == '__main__':
    lineitem = pysdql.DataFrame()
    orders = pysdql.DataFrame()
    nation = pysdql.DataFrame()
    supplier = pysdql.DataFrame()
    part = pysdql.DataFrame()
    partsupp = pysdql.DataFrame()

    tpch_q9(lineitem, orders, nation, supplier, part, partsupp).opt_to_sdqlir()

    test_one = pysdql.tpch_query(9, verbose=False)

    # print(pysdql.query.tpch.Qpandas.q21())

    # print(pysdql.query.tpch.Qsdql.q21())
