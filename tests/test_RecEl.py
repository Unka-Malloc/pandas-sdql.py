import unittest

from pysdql.core.dtypes.RecEl import RecEl


class MyTestCase(unittest.TestCase):

    def test_expr(self):
        rec1 = RecEl({'a': 'r_k.a', 'b': 'r_k.b'})
        self.assertEqual('<a = r_k.a, b = r_k.b>', rec1.expr)

        rec2 = RecEl({'a': 'Apple', 'b': 'Banana'}, mutable=False)
        self.assertEqual('<a = "Apple", b = "Banana">', rec2.expr)

    def test_fields(self):
        rec1 = RecEl({'a': 'r_k.a', 'b': 'r_k.b'})
        self.assertEqual(['a', 'b'], rec1.fields())

        rec2 = RecEl({'a': 'Apple', 'b': 'Banana'}, mutable=False)
        self.assertEqual(['a', 'b'], rec2.fields())

    def test_access(self):
        rec1 = RecEl({'a': 'r_k.a', 'b': 'r_k.b'})
        self.assertEqual('r_k.a', rec1.access('a'))
        self.assertEqual('r_k.b', rec1.access('b'))

    def test_getitem(self):
        rec1 = RecEl({'a': 'r_k.a', 'b': 'r_k.b'})
        self.assertEqual('r_k.a', rec1['a'])
        self.assertEqual('r_k.b', rec1['b'])

    def test_setitem(self):
        rec1 = RecEl({'a': 'r_k.a', 'b': 'r_k.b'})
        rec1['a'] = '"Apple"'
        self.assertEqual('"Apple"', rec1['a'])
        rec1['b'] = '"Banana"'
        self.assertEqual('"Banana"', rec1['b'])
        rec1['c'] = '"Car"'
        self.assertEqual('"Car"', rec1['c'])
        self.assertEqual('<a = "Apple", b = "Banana", c = "Car">', rec1.expr)


if __name__ == '__main__':
    unittest.main()
