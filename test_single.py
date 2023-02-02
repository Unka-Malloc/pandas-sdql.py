import pysdql

from pysdql.core.dtypes.EnumUtil import AggrType

from pysdql.query.tpch.template import *

if __name__ == '__main__':
    # supplier = pysdql.DataFrame()
    # nation = pysdql.DataFrame()
    # partsupp = pysdql.DataFrame()
    # part = pysdql.DataFrame()
    # lineitem = pysdql.DataFrame()
    #
    # tpch_q20(supplier, nation, partsupp, part, lineitem).opt_to_sdqlir()

    test_one = pysdql.tpch_query(20)
