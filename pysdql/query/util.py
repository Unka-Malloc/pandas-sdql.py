import numpy as np
from typing import List

import pandas

from pysdql.extlib.sdqlpy.sdql_lib import (sr_dict, record)

from pysdql.core.util.type_checker import is_date


def pandas_to_df(result):
    if isinstance(result, pandas.DataFrame):
        return result
    elif isinstance(result, pandas.Series):
        res_dict = {}
        i_count = 0
        res_index = []

        tmp_dict = result.to_dict()

        for k in tmp_dict.keys():
            res_dict[k] = [tmp_dict[k]]

            res_index.append(i_count)
            i_count += 1

        return pandas.DataFrame(tmp_dict, index=res_index)
    elif isinstance(result, float):
        return pandas.DataFrame({'result': [result]})
    else:
        raise NotImplementedError


def sdql_to_df(sdql_obj):
    if isinstance(sdql_obj, dict):
        if len(sdql_obj.keys()) == 0:
            raise NotImplementedError
        elif len(sdql_obj.keys()) == 1:
            sdql_rec = list(sdql_obj.keys())[0]

            if isinstance(sdql_rec, record):
                return pandas.DataFrame(sdql_record_to_pydict(sdql_rec))
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError
    elif isinstance(sdql_obj, sr_dict):
        container = sdql_obj.getContainer()

        if len(container.keys()) == 0:
            raise NotImplementedError
        else:
            res_list = []
            for k in container.keys():
                if isinstance(k, record):
                    res_list.append(sdql_record_to_pydict(k))
            else:
                return pandas.DataFrame(concat_pydict(res_list))
    elif isinstance(sdql_obj, float):
        return pandas.DataFrame({'result': [sdql_obj]})
    elif sdql_obj is None:
        return pandas.DataFrame({'result': [sdql_obj]})
    else:
        print(type(sdql_obj))
        raise NotImplementedError


def sdql_record_to_pydict(sdql_record: record):
    res_dict = {}

    container = sdql_record.getContainer()
    for k in container.keys():
        res_dict[k] = [container[k]]

    return res_dict


def concat_pydict(res_list: List[dict]):
    res_dict = {}

    for i in range(len(res_list)):
        if i == 0:
            for k in res_list[i].keys():
                res_dict[k] = res_list[i][k]
        else:
            for k in res_list[i].keys():
                res_dict[k] += res_list[i][k]

    return res_dict


def compare_dataframe(sdql_df: pandas.DataFrame, pandas_df: pandas.DataFrame):
    if sdql_df is None:
        if pandas_df is None:
            print('SDQL and Pandas results are both None!')
            return True
        else:
            print('Pandas result exists but SDQL result is None')
            return False
    else:
        if pandas_df is None:
            print('SDQL result exists but Pandas result is None')
            return False

    if sdql_df.shape[0] == pandas_df.shape[0]:
        print(f'Shape Check Passed: {sdql_df.shape[0]} rows x {sdql_df.shape[1]} columns')
    else:
        print('Mismatch Shape!')
        return False

    for c in sdql_df.columns:
        if sdql_df[c].dtype == np.float64:
            sdql_df[c] = sdql_df[c].astype(int)
            pandas_df[c] = pandas_df[c].astype(int)

    for c in pandas_df.columns:
        if pandas_df[c].dtype == object:
            if pandas_df[c].apply(lambda x: is_date(x)).all():
                pandas_df[c] = pandas_df[c].apply(lambda x: np.float64(x.replace('-', '')))

    for xi, xrow in sdql_df.iterrows():

        answer_df = pandas_df

        for k in xrow.keys():
            subset_df = answer_df[answer_df[k] == xrow[k]]
            if subset_df.empty:
                print(f'Not found {xrow.to_dict()}')
                print(f'While looking for {k} == {xrow[k]}')
                print(f'The answer is as following:')
                print(answer_df)
                return False
            else:
                answer_df = subset_df
        else:
            return True
    else:
        return True
