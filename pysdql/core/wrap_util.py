from typing import List
from pysdql.extlib.sdqlpy.sdql_lib import (sr_dict, record)

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

def sdql_record_to_pydict(sdql_record: record):
    res_dict = {}

    container = sdql_record.getContainer()
    for k in container.keys():
        res_dict[k] = [container[k]]

    return res_dict

def sdql_to_py(sdql_obj):
    if isinstance(sdql_obj, dict):
        if len(sdql_obj.keys()) == 0:
            raise NotImplementedError
        elif len(sdql_obj.keys()) == 1:
            sdql_rec = list(sdql_obj.keys())[0]

            if isinstance(sdql_rec, record):
                return {sdql_record_to_pydict(sdql_rec): sdql_obj[sdql_rec]}
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
                return concat_pydict(res_list)
    elif isinstance(sdql_obj, (int, float)):
        return sdql_obj
    elif sdql_obj is None:
        return sdql_obj
    else:
        print(type(sdql_obj))
        raise NotImplementedError