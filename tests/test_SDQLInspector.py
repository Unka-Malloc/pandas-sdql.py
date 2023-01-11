import unittest

from pysdql.core.dtypes.sdql_ir import *

from pysdql.core.dtypes.SDQLInspector import SDQLInspector


class MyTestCase(unittest.TestCase):
    def test_something(self):
        brand12 = VarExpr('brand12')
        smcase = VarExpr('smcase')
        smbox = VarExpr('smbox')
        smpack = VarExpr('smpack')
        smpkg = VarExpr('smpkg')
        brand23 = VarExpr('brand23')
        medbag = VarExpr('medbag')
        medbox = VarExpr('medbox')
        medpkg = VarExpr('medpkg')
        medpack = VarExpr('medpack')
        brand34 = VarExpr('brand34')
        lgcase = VarExpr('lgcase')
        lgbox = VarExpr('lgbox')
        lgpack = VarExpr('lgpack')
        lgpkg = VarExpr('lgpkg')
        air = VarExpr('air')
        airreg = VarExpr('airreg')
        deliverinperson = VarExpr('deliverinperson')

        bindings = LetExpr(brand12, ConstantExpr("Brand#12"),
                           LetExpr(smcase, ConstantExpr("SM CASE"),
                                   LetExpr(smbox, ConstantExpr("SM BOX"),
                                           LetExpr(smpack, ConstantExpr("SM PACK"),
                                                   LetExpr(smpkg, ConstantExpr("SM PKG"),
                                                           LetExpr(brand23, ConstantExpr("Brand#23"),
                                                                   LetExpr(medbag, ConstantExpr("MED BAG"),
                                                                           LetExpr(medbox, ConstantExpr("MED BOX"),
                                                                                   LetExpr(medpkg,
                                                                                           ConstantExpr("MED PKG"),
                                                                                           LetExpr(medpack,
                                                                                                   ConstantExpr(
                                                                                                       "MED PACK"),
                                                                                                   LetExpr(brand34,
                                                                                                           ConstantExpr(
                                                                                                               "Brand#34"),
                                                                                                           LetExpr(
                                                                                                               lgcase,
                                                                                                               ConstantExpr(
                                                                                                                   "LG CASE"),
                                                                                                               LetExpr(
                                                                                                                   lgbox,
                                                                                                                   ConstantExpr(
                                                                                                                       "LG BOX"),
                                                                                                                   LetExpr(
                                                                                                                       lgpack,
                                                                                                                       ConstantExpr(
                                                                                                                           "LG PACK"),
                                                                                                                       LetExpr(
                                                                                                                           lgpkg,
                                                                                                                           ConstantExpr(
                                                                                                                               "LG PKG"),
                                                                                                                           LetExpr(
                                                                                                                               air,
                                                                                                                               ConstantExpr(
                                                                                                                                   "AIR"),
                                                                                                                               LetExpr(
                                                                                                                                   airreg,
                                                                                                                                   ConstantExpr(
                                                                                                                                       "AIR REG"),
                                                                                                                                   LetExpr(
                                                                                                                                       deliverinperson,
                                                                                                                                       ConstantExpr(
                                                                                                                                           "DELIVER IN PERSON"),
                                                                                                                                       ConstantExpr(
                                                                                                                                           None)))))))))))))))))))
        print(SDQLInspector.findall_bindings(bindings))


if __name__ == '__main__':
    unittest.main()
