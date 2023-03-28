from pysdql.query.tpch.const import (LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE})
def query(li):

    # Insert
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (((((((((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) * (x[0].l_discount >= 0.05))) * (x[0].l_discount <= 0.07))) * (x[0].l_quantity < 24))) else (None))
    
    lineitem_1 = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey, "l_linenumber": x[0].l_linenumber, "l_quantity": x[0].l_quantity, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "l_tax": x[0].l_tax, "l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "l_shipdate": x[0].l_shipdate, "l_commitdate": x[0].l_commitdate, "l_receiptdate": x[0].l_receiptdate, "l_shipinstruct": x[0].l_shipinstruct, "l_shipmode": x[0].l_shipmode, "l_comment": x[0].l_comment}): True})
    
    results = lineitem_0.sum(lambda x: ((x[0].l_extendedprice) * (x[0].l_discount)))
    
    # Complete

    return results
