from datetime import datetime, timedelta

import pysdql

from pysdql.core.dtypes.CondExpr import CondExpr

if __name__ == '__main__':
    # query = 'let R = {<name="Apple"> -> {<name="Apple", initial="A"> -> 1}} in R(<name="Elephant">) == {}'
    # pysdql.db_driver(db_path=r'T:/sdql').run(query)

    # query = 'let R = {<name="Apple"> -> {<name="Apple", initial="A"> -> 1}} in R(<name="Elephant">) == {<> -> 1}'
    # pysdql.db_driver(db_path=r'T:/sdql').run(query)

    query = 'let R = {<name="Apple"> -> {<name="Apple", initial="A"> -> 1}} in R(<name="Elephant">) == 0'
    pysdql.db_driver(db_path=r'T:/sdql').run(query)

    # d1 = datetime.strptime('1998-12-01', "%Y-%m-%d")
    # d3 = d1 - timedelta(days=87)
    # print(d3.strftime("%Y-%m-%d"))

    'let R = sum (<c_k, c_v> in customer) ' \
    'sum (<o_k, o_v> in orders) ' \
    'sum (<l_k, l_v> in lineitem) ' \
    'sum (<s_k, s_v> in supplier) ' \
    'sum (<n_k, n_v> in nation) ' \
    'sum (<r_k, r_v> in region) ' \
    'if (((((((c_k.c_custkey == o_k.o_custkey) ' \
    '&& (l_k.l_orderkey == o_k.o_orderkey)) ' \
    '&& (l_k.l_suppkey == s_k.s_suppkey)) ' \
    '&& (c_k.c_nationkey == s_k.s_nationkey)) ' \
    '&& (s_k.s_nationkey == n_k.n_nationkey)) ' \
    '&& (n_k.n_regionkey == r_k.r_regionkey))) ' \
    'then { concat(concat(concat(concat(concat(c_k, o_k), l_k), s_k), n_k), r_k) -> c_v * o_v * l_v * s_v * n_v * r_v } else {  } ' \
    'sum(<r, r_v> in R) r_v'

    'let R1 = sum (<c_k, c_v> in customer) sum (<o_k, o_v> in orders) if((c_k.c_custkey == o_k.o_custkey)) then { concat(c_k, o_k) -> c_v * o_v } else {  }'
    'sum(<r, r_v> in R1) r_v'

    '''
    let R1 = sum (<c_k, c_v> in customer) sum (<o_k, o_v> in orders) if((c_k.c_custkey == o_k.o_custkey)) then { concat(c_k, o_k) -> c_v * o_v } else {  }

    let R2 = sum(<r_k, r_v> in R1) sum (<l_k, l_v> in lineitem) if((l_k.l_orderkey == o_k.o_orderkey)) then { concat(r_k, l_k) -> c_v * o_v } else {  }
    sum(<r, r_v> in R2) r_v
    '''

    # lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)

    #

    # count = 0
    # with open(r'T:/UG4-Proj/datasets/lineitem.tbl', 'r') as f:
    #     line = f.readline()
    #     while line:
    #         line_list = line.split('|')
    #
    #         if 0.06 <= float(line_list[6]) <= 0.08:
    #             count += 1
    #
    #         line = f.readline()
    #     else:
    #         print(count)



    'let customer = load[{<c_custkey: int, c_name: string, c_address: string, c_nationkey: int, c_phone: string, c_acctbal: real, c_mktsegment: string, c_comment: string> -> int}]("T:/sdql/datasets/tuned/customer.tbl")'

    'let customer = load[{<c_custkey: int, c_name: string, c_address: string, c_nationkey: int, c_phone: string, c_acctbal: real, c_mktsegment: string, c_comment: string> -> int}]("T:/UG4-Proj/datasets/customer.tbl")'