from datetime import datetime

import pysdql

from pysdql.core.dtypes.ConditionalUnit import CondUnit

if __name__ == '__main__':
    var1 = '1998-12-01'
    r = pysdql.relation('R')
    print(r['A'] > var1)