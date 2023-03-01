import pandas as pd
# import pysdql as pd
from pysdql import tosdql

if __name__ == '__main__':
    li = pd.read_csv(f"T:/tpch_dataset/100M/lineitem.tbl",
                     sep='|',
                     header=None,
                     names=['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                            'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
                            'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'],
                     index_col=False,
                     dtype={"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int,
                            "l_quantity": float, "l_extendedprice": float, "l_discount": float, "l_tax": float,
                            "l_returnflag": str, "l_linestatus": str, "l_shipinstruct": str, "l_shipmode": str,
                            "l_comment": str},
                     parse_dates=['l_shipdate', 'l_commitdate', 'l_receiptdate'])

    print(li.groupby('l_orderkey').agg({'l_orderkey': 'count'}))


    # @tosdql
    # def query(lineitem):
    #     # lineitem['revenue'] = lineitem.l_extendedprice * lineitem.l_discount
    #     # result = lineitem.agg({'revenue': 'sum'})
    #
    #     empty_df = pd.DataFrame()
    #     empty_df['revenue'] = [(lineitem.l_extendedprice * lineitem.l_discount).sum()]
    #     result = empty_df[['revenue']]
    #
    #     # result = (lineitem.l_extendedprice * lineitem.l_discount).sum()
    #     return result
    #
    # print(query(li))
