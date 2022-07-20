import subprocess

import pysdql

if __name__ == '__main__':
    db_driver = pysdql.db_driver(db_path=f'T:/sdql')

    # r = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)

    query = '''
    let lineitem = load[{<l_orderkey: int, l_partkey: int, l_suppkey: int, l_linenumber: int, l_quantity: double, l_extendedprice: double, l_discount: double, l_tax: double, l_returnflag: string, l_linestatus: string, l_shipdate: date, l_commitdate: date, l_receiptdate: date, l_shipinstruct: string, l_shipmode: string, l_comment: string> -> int}]("datasets/tpch/lineitem.tbl")
    lineitem
    '''

    db_driver.run(query)








