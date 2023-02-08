import pysdql

from pysdql.query.tpch.template import *

if __name__ == '__main__':
    part = pysdql.DataFrame()
    supplier = pysdql.DataFrame()
    partsupp = pysdql.DataFrame()
    nation = pysdql.DataFrame()
    region = pysdql.DataFrame()

    tpch_q2(part, supplier, partsupp, nation, region).opt_to_sdqlir()

    # test_one = pysdql.tpch_query(2)

    # print(pysdql.query.tpch.Qpandas.q2())
