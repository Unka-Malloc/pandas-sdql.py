def select(condlist, choicelist, default=0):
    if not type(condlist) == list and type(choicelist) == list:
        raise TypeError()
    if not len(condlist) == len(choicelist):
        raise TypeError

    from pysdql.core.api import (CaseExpr)
    if len(condlist) == 1 and len(choicelist) == 1:
        return CaseExpr(condlist[0], choicelist[0], default)
