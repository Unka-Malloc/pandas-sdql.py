import pysdql

if __name__ == '__main__':
    pysdql.tune_tbl(r'T:\sdql\datasets\tpch\customer.tbl')
    pysdql.tune_tbl(r'T:\sdql\datasets\tpch\lineitem.tbl')
    pysdql.tune_tbl(r'T:\sdql\datasets\tpch\nation.tbl')
    pysdql.tune_tbl(r'T:\sdql\datasets\tpch\orders.tbl')
    pysdql.tune_tbl(r'T:\sdql\datasets\tpch\part.tbl')
    pysdql.tune_tbl(r'T:\sdql\datasets\tpch\partsupp.tbl')
    pysdql.tune_tbl(r'T:\sdql\datasets\tpch\region.tbl')
    pysdql.tune_tbl(r'T:\sdql\datasets\tpch\supplier.tbl')

