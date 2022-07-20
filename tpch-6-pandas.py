import pandas

import pysdql

if __name__ == '__main__':
    path = r'T:\UG4-Proj\datasets'
    lineitem = pandas.read_table(rf'{path}\lineitem.tbl', sep='|', index_col=False, header=None,
                                 names=pysdql.LINEITEM_COLS)

    # r = lineitem[(lineitem.l_shipdate >= '1993-01-01') & (lineitem.l_shipdate < '1994-01-01')]  # 865

    # r = lineitem[(lineitem['l_discount'] > 0.06)
    #              & (lineitem.l_discount < 0.08)
    #              & (lineitem.l_quantity < 25)]

    r = lineitem[(lineitem['l_discount'] >= 0.06) & (lineitem['l_discount'] <= 0.08)]

    # r = lineitem[lineitem['l_quantity'] < 25]

    print(len(r))
