import unittest

from pysdql.core.exprs.carrier.LoadExpr import LoadExpr


class MyTestCase(unittest.TestCase):
    def test_expr(self):
        self.assertEqual('load[{<ID: int, Name: string, Age: int, Birthday: date, Grade: real> -> int}]("test.csv")',
                         LoadExpr({'ID': 'int', 'Name': 'string', 'Age': 'int', 'Birthday': 'date', 'Grade': 'real'},
                                  'test.csv').expr)


if __name__ == '__main__':
    unittest.main()
