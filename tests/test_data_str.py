import unittest

from pysdql.core.util.data_str import (
    remove_prefix,
    remove_suffix,
    remove_sides,
)


class MyTestCase(unittest.TestCase):
    def test_remove_prefix(self):
        self.assertEqual('.csv', remove_prefix('test.csv', 'test'))
        self.assertEqual('hello', remove_prefix(' hello', ' '))

    def test_remove_suffix(self):
        self.assertEqual('test', remove_suffix('test', '.csv'))
        self.assertEqual('hello', remove_suffix('hello ', ' '))

    def test_remove_sides(self):
        self.assertEqual('Apple', remove_sides('"Apple"', '"'))
        self.assertEqual('Banana', remove_sides(' Banana ', ' '))
        self.assertEqual('Hello, World', remove_sides(' Hello, World ', ' '))


if __name__ == '__main__':
    unittest.main()
