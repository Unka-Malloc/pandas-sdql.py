import pysdql

if __name__ == '__main__':
    data_path = r'T:\UG4-Proj\datasets\1G'

    pysdql.tune_tbl(rf'{data_path}\customer.tbl')
    pysdql.tune_tbl(rf'{data_path}\lineitem.tbl')
    pysdql.tune_tbl(rf'{data_path}\nation.tbl')
    pysdql.tune_tbl(rf'{data_path}\orders.tbl')
    pysdql.tune_tbl(rf'{data_path}\part.tbl')
    pysdql.tune_tbl(rf'{data_path}\partsupp.tbl')
    pysdql.tune_tbl(rf'{data_path}\region.tbl')
    pysdql.tune_tbl(rf'{data_path}\supplier.tbl')

