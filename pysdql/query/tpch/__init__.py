import time

import pysdql.query.tpch.Qsdql

import pysdql.query.tpch.Qpandas

from pysdql.query.util import sdql_to_df, pandas_to_df, compare_dataframe


def tpch_query(qindex=1, execution_mode=0, threads_count=1) -> bool:
    done = [1, 6]

    if isinstance(qindex, float):
        if qindex not in done:
            print(f'Query {qindex} has not been verified.')
            return False

        qindex = [qindex]

    if isinstance(qindex, list):
        check_list = []
        for q in qindex:
            if q not in done:
                print(f'Query {q} has not been verified.')
                return False

            sdql_result = eval(f'pysdql.query.tpch.Qsdql.q{q}({execution_mode}, {threads_count})')

            pandas_result = eval(f'pysdql.query.tpch.Qpandas.q{q}()')

            sdql_df = sdql_to_df(sdql_result)

            pandas_df = pandas_to_df(pandas_result)

            print(f'>> Query {q} <<')

            print(f'>> SDQL <<')

            print(sdql_result)

            print(f'>> Pandas <<')

            print(pandas_result)

            sep_line = '#' * 75

            if compare_dataframe(sdql_df, pandas_df):
                check_list.append(True)
                print(sep_line)
                print(f'''
                {art_map[q]}
                ███████╗██╗   ██╗ ██████╗ ██████╗███████╗███████╗███████╗
                ██╔════╝██║   ██║██╔════╝██╔════╝██╔════╝██╔════╝██╔════╝
                ███████╗██║   ██║██║     ██║     █████╗  ███████╗███████╗
                ╚════██║██║   ██║██║     ██║     ██╔══╝  ╚════██║╚════██║
                ███████║╚██████╔╝╚██████╗╚██████╗███████╗███████║███████║
                ╚══════╝ ╚═════╝  ╚═════╝ ╚═════╝╚══════╝╚══════╝╚══════╝                                             
                ''')
                print(sep_line)
            else:
                check_list.append(False)
                print(sep_line)
                print(f'''
                {art_map[q]}
                ███████╗ █████╗ ██╗██╗     ██╗   ██╗██████╗ ███████╗
                ██╔════╝██╔══██╗██║██║     ██║   ██║██╔══██╗██╔════╝
                █████╗  ███████║██║██║     ██║   ██║██████╔╝█████╗  
                ██╔══╝  ██╔══██║██║██║     ██║   ██║██╔══██╗██╔══╝  
                ██║     ██║  ██║██║███████╗╚██████╔╝██║  ██║███████╗
                ╚═╝     ╚═╝  ╚═╝╚═╝╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚══════╝                               
                ''')
                print(sep_line)
        else:
            return all(check_list)
    else:
        raise NotImplementedError


art_map = {
    1: '''
 ██████╗ ██╗   ██╗███████╗██████╗ ██╗   ██╗     ██╗
██╔═══██╗██║   ██║██╔════╝██╔══██╗╚██╗ ██╔╝    ███║
██║   ██║██║   ██║█████╗  ██████╔╝ ╚████╔╝     ╚██║
██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗  ╚██╔╝       ██║
╚██████╔╝╚██████╔╝███████╗██║  ██║   ██║        ██║
 ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝        ╚═╝
    ''',
    6: '''
 ██████╗ ██╗   ██╗███████╗██████╗ ██╗   ██╗     ██████╗ 
██╔═══██╗██║   ██║██╔════╝██╔══██╗╚██╗ ██╔╝    ██╔════╝ 
██║   ██║██║   ██║█████╗  ██████╔╝ ╚████╔╝     ███████╗ 
██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗  ╚██╔╝      ██╔═══██╗
╚██████╔╝╚██████╔╝███████╗██║  ██║   ██║       ╚██████╔╝
 ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝        ╚═════╝ 

    ''',
}