import pysdql

if __name__ == '__main__':
    db_driver = pysdql.driver(db_path=f'T:/sdql')

    r = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)

    db_driver.get(r)
