import numpy as np
from typing import List

import pandas

from pysdql.extlib.sdqlpy.sdql_lib import (sr_dict, record)

from pysdql.core.utils.type_checker import is_date


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
        raise NotImplementedError(result)

def sdql_to_df(sdql_obj, is_agg=False):
    if isinstance(sdql_obj, dict):
        if len(sdql_obj.keys()) == 0:
            raise NotImplementedError
        elif len(sdql_obj.keys()) == 1:
            sdql_rec = list(sdql_obj.keys())[0]

            if isinstance(sdql_rec, record):
                if is_agg:
                    return sdql_record_to_series(sdql_rec)

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
    elif isinstance(sdql_obj, (int, float)):
        if is_agg:
            return sdql_obj

        return pandas.DataFrame({'result': [sdql_obj]})
    elif sdql_obj is None:
        if is_agg:
            return sdql_obj

        return pandas.DataFrame()
    else:
        print(type(sdql_obj))
        raise NotImplementedError


def sdql_record_to_pydict(sdql_record: record):
    res_dict = {}

    container = sdql_record.getContainer()
    for k in container.keys():
        res_dict[k] = [container[k]]

    return res_dict

def sdql_record_to_series(sdql_record: record):
    res_dict = {}

    container = sdql_record.getContainer()
    for k in container.keys():
        res_dict[k] = container[k]

    return pandas.Series(res_dict)


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


def compare_dataframe(sdql_df: pandas.DataFrame, pd_df: pandas.DataFrame, verbose=False, for_duck=False):
    print('=' * 60)
    if for_duck:
        print('>> Comparing Pandas with Duck ... <<')
    else:
        print('>> Comparing SDQL with Pandas ... <<')

    sdql_df = sdql_df.copy()
    pd_df = pd_df.copy()

    if sdql_df is None:
        if pd_df is None:
            print('SDQL and Pandas results are both None!')
            return True
        else:
            print('Pandas result exists but SDQL result is None')
            return False
    else:
        if pd_df is None:
            print('SDQL result exists but Pandas result is None')
            return False

    if sdql_df.shape[0] == 1:
        if pd_df.empty:
            if len(sdql_df.columns) == len(pd_df.columns):
                return False
            else:
                try:
                    if sdql_df.columns == ['result']:
                        if sdql_df['result'].loc[0] is None:
                            return True
                except:
                    return False

        if pd_df.shape[0] == 1:
            if sdql_df.squeeze() is None:
                return False
            try:
                if int(sdql_df.squeeze()) == int(pd_df.squeeze()):
                    return True
            except:
                print(f'Warning: squeeze failed')
                # print(sdql_df)
                # print(pd_df)

    if sdql_df.shape[0] == pd_df.shape[0]:
        if verbose:
            print(f'Shape Check Passed: {sdql_df.shape[0]} rows x {sdql_df.shape[1]} columns')
    else:
        print(f'Warning: Mismatch Shape: {{SDQL: {sdql_df.shape[0]}, Pandas: {pd_df.shape[0]}}}')

        if sdql_df.shape[0] < pd_df.shape[0]:
            # compatible with SQL pipeline
            print(f'Warning: DF 1 (SDQL) is a subset of DF 2 (Pandas)')
            # return False
        else:
            if not for_duck:
                return False

    mul_float_factor = 1000
    mul_float_list = []

    round_float_list = []

    for c in sdql_df.columns:
        if c.endswith('_NA'):
            continue
        if c not in pd_df.columns:
            print(f'Warning: Column {c} not found!')
            continue
        if sdql_df[c].dtype == np.float64:
            if sdql_df[c].apply(lambda x: x < np.float64(1.0)).all():
                sdql_df[c] = sdql_df[c].apply(lambda x: x * mul_float_factor).astype(int)
                mul_float_list.append(c)
            else:
                sdql_df[c] = sdql_df[c].round(1)
                round_float_list.append(c)
        elif sdql_df[c].dtype == object:
            if sdql_df[c].apply(lambda x: is_date(x)).all():
                sdql_df[c] = sdql_df[c].apply(lambda x: np.float64(x.replace('-', '')))

    for c in pd_df.columns:
        if c in mul_float_list:
            pd_df[c] = pd_df[c].apply(lambda x: x * mul_float_factor).astype(int)
        elif c in round_float_list:
            pd_df[c] = pd_df[c].round(1)
        elif pd_df[c].dtype == object:
            if pd_df[c].apply(lambda x: is_date(x)).all():
                pd_df[c] = pd_df[c].apply(lambda x: np.float64(x.replace('-', '')))

    verified_count = 0
    mismatch_count = 0

    for xi, xrow in sdql_df.iterrows():
        answer_df = pd_df

        row_success = False

        for k in xrow.keys():
            if k.endswith('_NA'):
                continue
            if k not in pd_df.columns:
                continue
            subset_df = answer_df[answer_df[k] == xrow[k]]
            if subset_df.empty:
                if not for_duck:
                    print(f'At row number {verified_count} / {sdql_df.shape[0]}')
                    print(f'Not found {xrow.to_dict()}')
                    print(f'Failed while looking for {k} == {xrow[k]}')
                    print(f'The answer is as following:')
                    print(answer_df)
                # return False

                row_success = False
            else:
                answer_df = subset_df

                row_success = True

        else:
            if row_success:
                verified_count += 1
            else:
                mismatch_count += 1

            if verbose:
                # print(f'Success Verify {xrow.to_dict()}')
                pass
    else:
        if mismatch_count == 0:
            return True
        else:
            if not for_duck:
                print(f'number of mismatch records: {mismatch_count}')
                return False

    return True

def exists_duplicates(test_str: str):
    i = 0

    for j in range(len(test_str)):
        if test_str[i:j] == test_str[j:j + j - i]:
            singleton = test_str[i:j]
            if len(singleton.strip()) > 0:
                return True
    else:
        return False

def remove_duplicates(dup_str: str):
    i = 0

    for j in range(len(dup_str)):
        if dup_str[i:j] == dup_str[j:j+j-i]:
            singleton = dup_str[i:j]
            if len(singleton.strip()) > 0:
                return singleton
    else:
        return dup_str
