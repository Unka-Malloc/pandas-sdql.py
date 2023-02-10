from pysdql.core.dtypes.sdql_ir import ExtFuncSymbol
from pysdql.core.dtypes import CondExpr, ColExtExpr


def is_cond(flex_obj):
    if isinstance(flex_obj, CondExpr):
        return True
    if isinstance(flex_obj, ColExtExpr):
        if flex_obj.func in [ExtFuncSymbol.StartsWith,
                             ExtFuncSymbol.EndsWith,
                             ExtFuncSymbol.StringContains]:
            return True

    return False

def map_name_to_dataset(name):
    if name == 'customer':
        return ("db->cu_dataset")
    if name == 'lineitem':
        return ("db->li_dataset")
    if name == 'orders':
        return ("db->ord_dataset")
    if name == 'nation':
        return ('db->na_dataset')
    if name == 'region':
        return ('db->re_dataset')
    if name == 'part':
        return ("db->pa_dataset")
    if name == 'supplier':
        return ('db->su_dataset')
    if name == 'partsupp':
        return ('db->ps_dataset')
    return name
