import unittest

import pysdql
from pysdql.core.dtypes.ValExpr import ValExpr


class MyTestCase(unittest.TestCase):
    @property
    def df(self):
        return pysdql.DataFrame({})

    def test_sum(self):
        v = self.df['A'].sum()
        print(v)
        self.assertEqual(True, type(v) == ValExpr)

    def test_count(self):
        v = self.df['A'].count()
        print(v)
        self.assertEqual(True, type(v) == ValExpr)

    def test_mean(self):
        v = self.df['A'].mean()
        print(v)
        self.assertEqual(True, type(v) == ValExpr)

    def test_min(self):
        v = self.df['A'].min()
        print(v)
        self.assertEqual(True, type(v) == ValExpr)

    def test_max(self):
        v = self.df['A'].max()
        print(v)
        self.assertEqual(True, type(v) == ValExpr)


if __name__ == '__main__':
    unittest.main()
