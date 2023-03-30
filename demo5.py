from pysdql.core.prototype.basic.sdql_ir import *

from pysdql.extlib.sdqlir_to_sdqlpy import GenerateSDQLPYCode

if __name__ == '__main__':
    a = LetExpr(VarExpr('a'),
                JoinProbeBuilder(VarExpr('b'),
                                 VarExpr('c'),
                                 "l_partkey",
                                 lambda p: ConstantExpr(True),
                                 lambda indexedDictValue, probeDictKey:
                                 IfExpr(((ConstantExpr(0.2) * (indexedDictValue.l_quantity / indexedDictValue.count)) > probeDictKey.l_quantity),
                                        probeDictKey.l_extendedprice,
                                        ConstantExpr(0.0))),
                ConstantExpr(True))

    print(GenerateSDQLPYCode(a, {}))