import unittest

from pysdql.core.dtypes.DictEl import DictEl


class MyTestCase(unittest.TestCase):
    def test_expr(self):
        dict1 = DictEl({'<A=r_k.a>': '<B=r_k.b * r_v>'})
        self.assertEqual('{<A=r_k.a> -> <B=r_k.b * r_v>}', dict1.expr)

        dict2 = DictEl({'a': 'Apple', 'b': 'Banana'}, mutable=False)
        self.assertEqual('{"a" -> "Apple", "b" -> "Banana"}', dict2.expr)


if __name__ == '__main__':
    unittest.main()
