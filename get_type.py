import pysdql

if __name__ == '__main__':
    pysdql.get_load(r'T:\sdql\datasets\tpch\customer.tbl', pysdql.CUSTOMER_COLS)
    pysdql.get_load(r'T:\sdql\datasets\tpch\lineitem.tbl', pysdql.LINEITEM_COLS)
    pysdql.get_load(r'T:\sdql\datasets\tpch\nation.tbl', pysdql.NATION_COLS)
    pysdql.get_load(r'T:\sdql\datasets\tpch\orders.tbl', pysdql.ORDERS_COLS)
    pysdql.get_load(r'T:\sdql\datasets\tpch\part.tbl', pysdql.PART_COLS)
    pysdql.get_load(r'T:\sdql\datasets\tpch\partsupp.tbl', pysdql.PARTSUPP_COLS)
    pysdql.get_load(r'T:\sdql\datasets\tpch\region.tbl', pysdql.REGION_COLS)
    pysdql.get_load(r'T:\sdql\datasets\tpch\supplier.tbl', pysdql.SUPPLIER_COLS)