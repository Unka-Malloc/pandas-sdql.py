import pysdql
import pandas as pd
from pysdql.query.tpch.const import DATAPATH, LINEITEM_COLS

if __name__ == '__main__':
    test_all = pysdql.tpch_query([1, 3, 4, 6, 10, 14, 15, 16, 18, 19], 0, 1)
