from pysdql.extlib.sdqlpy.sdql_lib import *
@sdql_compile({"li": {record({"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int, "l_quantity": float, "l_extendedprice": float, "l_discount": float, "l_tax": float, "l_returnflag": string(256), "l_linestatus": string(256), "l_shipdate": date, "l_commitdate": date, "l_receiptdate": date, "l_shipinstruct": string(256), "l_shipmode": string(256), "l_comment": string(256), "_NA": string(1)}): bool}})
def query(li):
    li_aggr = li.sum(lambda x_li: {record({"l_orderkey": x_li[0].l_orderkey, "l_suppkey": x_li[0].l_suppkey}): x_li[0].l_quantity})
    
    results = li_aggr.sum(lambda x_li_aggr: {record({"l_orderkey": x_li_aggr[0].l_orderkey, "l_suppkey": x_li_aggr[0].l_suppkey, "l_quantity": x_li_aggr[1]}): True})
    
    return results
