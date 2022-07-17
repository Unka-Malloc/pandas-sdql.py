from datetime import datetime, timedelta

import pysdql

from pysdql.core.dtypes.ConditionalUnit import CondUnit

if __name__ == '__main__':
    d1 = datetime.strptime('1998-12-01', "%Y-%m-%d")
    d3 = d1 - timedelta(days=87)
    print(d3.strftime("%Y-%m-%d"))


