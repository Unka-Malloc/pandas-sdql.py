import time

import pysdql.query.tpch.Qsdql

import pysdql.query.tpch.Qpandas

from pysdql.query.util import sdql_to_df, pandas_to_df, compare_dataframe

def tpch_query(qindex=1, execution_mode=0, threads_count=1):
    done = [1, 6]

    if qindex not in done:
        print(f'Query {qindex} has not been verified.')
        return

    sdql_result = eval(f'pysdql.query.tpch.Qsdql.q{qindex}({execution_mode}, {threads_count})')

    pandas_result = eval(f'pysdql.query.tpch.Qpandas.q{qindex}()')

    sdql_df = sdql_to_df(sdql_result)

    pandas_df = pandas_to_df(pandas_result)

    print('<- SDQL ->')

    print(sdql_result)

    print('<- Pandas ->')

    print(pandas_result)

    if compare_dataframe(sdql_df, pandas_df):
        print('''
        ███████╗██╗   ██╗ ██████╗ ██████╗███████╗███████╗███████╗
        ██╔════╝██║   ██║██╔════╝██╔════╝██╔════╝██╔════╝██╔════╝
        ███████╗██║   ██║██║     ██║     █████╗  ███████╗███████╗
        ╚════██║██║   ██║██║     ██║     ██╔══╝  ╚════██║╚════██║
        ███████║╚██████╔╝╚██████╗╚██████╗███████╗███████║███████║
        ╚══════╝ ╚═════╝  ╚═════╝ ╚═════╝╚══════╝╚══════╝╚══════╝                                             
        ''')
    else:
        print('''
        ███████╗ █████╗ ██╗██╗     ██╗   ██╗██████╗ ███████╗
        ██╔════╝██╔══██╗██║██║     ██║   ██║██╔══██╗██╔════╝
        █████╗  ███████║██║██║     ██║   ██║██████╔╝█████╗  
        ██╔══╝  ██╔══██║██║██║     ██║   ██║██╔══██╗██╔══╝  
        ██║     ██║  ██║██║███████╗╚██████╔╝██║  ██║███████╗
        ╚═╝     ╚═╝  ╚═╝╚═╝╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚══════╝                               
        ''')
