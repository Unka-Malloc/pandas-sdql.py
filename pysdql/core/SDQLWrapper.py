from functools import wraps
from pysdql.core.dtypes.DataFrame import DataFrame as SdqlDataFrame
from pysdql.extlib.sdqlpy.sdql_lib import sdqlpy_init


def SDQL(func):
    @wraps(func)
    def SDQLWapper(*args, **kwargs):
        print(args, kwargs)
        result = func(*args)
        if isinstance(result, SdqlDataFrame):
            sdqlpy_init(0, 1)
            return result.run_in_sdql()
        return result

    return SDQLWapper
