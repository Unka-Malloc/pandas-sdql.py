import unittest

from pysdql.core.dtypes.DataFrame import DataFrame

from pysdql.core.dtypes.sdql_ir import PrintAST, GenerateSDQLCode


class MyTestCase(unittest.TestCase):
    def test_Q1(self):
        li = DataFrame()
        cond = li.l_shipdate <= "1998-09-02"
        print(cond)
        print(GenerateSDQLCode(cond))
        PrintAST(cond)
        self.assertEqual(True, True)

    def test_Q6(self):
        li = DataFrame()
        cond = ((li.l_shipdate >= "1994-01-01") &
                (li.l_shipdate < "1995-01-01") &
                (li.l_discount >= 0.05) &
                (li.l_discount <= 0.07) &
                (li.l_quantity < 24))
        print(cond.sdql_ir)
        print(GenerateSDQLCode(cond.sdql_ir))
        PrintAST(cond.sdql_ir)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
