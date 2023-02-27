from functools import wraps
from pysdql.config import is_pandas_available
from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.DataFrame import DataFrame as SdqlDataFrame
from pysdql.query.util import (
    sdql_to_df,
    sdql_to_py
)
from pysdql.extlib.sdqlpy.sdql_lib import sdqlpy_init


def tosdql(func):
    @wraps(func)
    def SDQLWapper(*args, **kwargs):
        # print(args, kwargs)
        flex_result = func(*args)
        if isinstance(flex_result, (SdqlDataFrame,
                                    AggrExpr,
                                    )):
            sdqlpy_init(0, 1)
            raw_sdql = flex_result.run_in_sdql(args)
            if is_pandas_available:
                return sdql_to_df(raw_sdql, is_agg=flex_result.ret_for_agg())
            else:
                return sdql_to_py(raw_sdql)
        return flex_result

    return SDQLWapper
