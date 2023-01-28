import numpy as np
from typing import List

import pandas

from pysdql.extlib.sdqlpy.sdql_lib import (sr_dict, record)


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
        elif len(container.keys()) == 1:
            raise NotImplementedError
        else:
            res_list = []
            for k in container.keys():
                if isinstance(k, record):
                    res_list.append(sdql_record_to_pydict(k))
            else:
                return pandas.DataFrame(concat_pydict(res_list))
    else:
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
    if sdql_df.shape[0] != pandas_df.shape[0]:
        return False

    for c in sdql_df.columns:
        if sdql_df[c].dtype == np.float64:
            sdql_df[c] = sdql_df[c].round(2)
            pandas_df[c] = pandas_df[c].round(2)

    for xi, xrow in sdql_df.iterrows():
        found_row = False
        for yi, yrow in pandas_df.iterrows():
            if xrow.isin(yrow).all():
                found_row = True
                continue
        else:
            if not found_row:
                print('Not Found')
                print(xrow)
                return False
    else:
        return True
