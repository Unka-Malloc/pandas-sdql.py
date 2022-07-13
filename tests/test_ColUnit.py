import unittest
from pysdql import relation
from pysdql.core.dtypes.ColumnUnit import ColUnit


class MyTestCase(unittest.TestCase):
    @property
    def relation(self):
        return relation('R')

    def test_selection(self):
        a = self.relation['A']

    def test_get_col(self):
        a = self.relation['A'] > 1

    def test_not(self):
        print(~ (self.relation['A'] < 1))


if __name__ == '__main__':
    unittest.main()
