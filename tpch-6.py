from pysdql.extlib.sdqlpy.lib.sdql_ir import *
from pysdql.extlib.sdqlir_to_sdqlpy import GenerateSDQLPYCode
from pysdql.extlib.sdqlpy.sdql_lib import *

if __name__ == '__main__':
    # lineitem = VarExpr('db->li_dataset')
    # x_lineitem = VarExpr('x_lineitem')
    # lineitem_aggr = VarExpr('lineitem_aggr')
    # results = VarExpr('results')
    #
    # query = LetExpr(lineitem_aggr, SumExpr(x_lineitem, lineitem, IfExpr(MulExpr(MulExpr(MulExpr(MulExpr(
    #     CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_lineitem, 0), 'l_shipdate'),
    #                 ConstantExpr(19940101)),
    #     CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_lineitem, 0), 'l_shipdate'),
    #                 ConstantExpr(19950101))), CompareExpr(CompareSymbol.GTE,
    #                                                       RecAccessExpr(PairAccessExpr(x_lineitem, 0), 'l_discount'),
    #                                                       ConstantExpr(0.05))), CompareExpr(CompareSymbol.LTE,
    #                                                                                         RecAccessExpr(
    #                                                                                             PairAccessExpr(
    #                                                                                                 x_lineitem, 0),
    #                                                                                             'l_discount'),
    #                                                                                         ConstantExpr(0.07))),
    #     CompareExpr(CompareSymbol.LT,
    #                 RecAccessExpr(
    #                     PairAccessExpr(
    #                         x_lineitem, 0),
    #                     'l_quantity'),
    #                 ConstantExpr(24))), MulExpr(
    #     RecAccessExpr(PairAccessExpr(x_lineitem, 0), 'l_extendedprice'),
    #     RecAccessExpr(PairAccessExpr(x_lineitem, 0), 'l_discount')), ConstantExpr(0.0)), False),
    #                 LetExpr(results, DicConsExpr([(RecConsExpr([('revenue', lineitem_aggr)]), ConstantExpr(True))]),
    #                         ConstantExpr(True)))
    #
    # query_string = str(GenerateSDQLPYCode(query, {}))
    #
    # test_string = '\n'.join([i for i in query_string.split('\n') if i != 'True' and i.strip() != ''])
    #
    # print(test_string)

    sdqlpy_init(0, 1)

    # The following path must point to your dbgen dataset.
    dataset_path = r'T:/tpch_dataset/1G/'

    # Shows the number of returned results, average and stdev of run time,
    # and the results (if the next parameter is also set to True)
    verbose = True
    show_results = True

    # Number of iterations for benchmarking each query (must be >=2)
    iterations = 2

    lineitem_type = {record(
        {"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int, "l_quantity": float,
         "l_extendedprice": float, "l_discount": float, "l_tax": float, "l_returnflag": string(1),
         "l_linestatus": string(1), "l_shipdate": date, "l_commitdate": date, "l_receiptdate": date,
         "l_shipinstruct": string(25), "l_shipmode": string(10), "l_comment": string(44), "l_NA": string(1)}): bool}
    customer_type = {record(
        {"c_custkey": int, "c_name": string(25), "c_address": string(40), "c_nationkey": int, "c_phone": string(15),
         "c_acctbal": float, "c_mktsegment": string(10), "c_comment": string(117), "c_NA": string(1)}): bool}
    order_type = {record(
        {"o_orderkey": int, "o_custkey": int, "o_orderstatus": string(1), "o_totalprice": float, "o_orderdate": date,
         "o_orderpriority": string(15), "o_clerk": string(15), "o_shippriority": int, "o_comment": string(79),
         "o_NA": string(1)}): bool}
    nation_type = {record({"n_nationkey": int, "n_name": string(25), "n_regionkey": int, "n_comment": string(152),
                           "n_NA": string(1)}): bool}
    region_type = {
        record({"r_regionkey": int, "r_name": string(25), "r_comment": string(152), "r_NA": string(1)}): bool}
    part_type = {record(
        {"p_partkey": int, "p_name": string(55), "p_mfgr": string(25), "p_brand": string(10), "p_type": string(25),
         "p_size": int, "p_container": string(10), "p_retailprice": float, "p_comment": string(23),
         "p_NA": string(1)}): bool}
    partsupp_type = {record(
        {"ps_partkey": int, "ps_suppkey": int, "ps_availqty": float, "ps_supplycost": float, "ps_comment": string(199),
         "ps_NA": string(1)}): bool}
    supplier_type = {record(
        {"s_suppkey": int, "s_name": string(25), "s_address": string(40), "s_nationkey": int, "s_phone": string(15),
         "s_acctbal": float, "s_comment": string(101), "s_NA": string(1)}): bool}

    lineitem = read_csv(dataset_path + "lineitem.tbl", lineitem_type, "li")
    customer = read_csv(dataset_path + "customer.tbl", customer_type, "cu")
    order = read_csv(dataset_path + "orders.tbl", order_type, "ord")
    nation = read_csv(dataset_path + "nation.tbl", nation_type, "na")
    region = read_csv(dataset_path + "region.tbl", region_type, "re")
    part = read_csv(dataset_path + "part.tbl", part_type, "pa")
    partsupp = read_csv(dataset_path + "partsupp.tbl", partsupp_type, "ps")
    supplier = read_csv(dataset_path + "supplier.tbl", supplier_type, "su")


    @sdql_compile({"li": lineitem_type})
    def q1(li):
        lineitem_aggr = li.sum(lambda x_lineitem: (((x_lineitem[0].l_extendedprice) * (x_lineitem[0].l_discount))) if ((
                    (((((((x_lineitem[0].l_shipdate >= 19940101) * (x_lineitem[0].l_shipdate < 19950101))) * (
                                x_lineitem[0].l_discount >= 0.05))) * (x_lineitem[0].l_discount <= 0.07))) * (
                        x_lineitem[0].l_quantity < 24))) else (0.0))
        results = {record({"revenue": lineitem_aggr}): True}

        return results


    print(q1(lineitem))
