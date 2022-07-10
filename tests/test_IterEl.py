import unittest
from pysdql.core.dtypes.IterationElement import IterEl


class MyTestCase(unittest.TestCase):
    @property
    def element(self):
        return IterEl(data='x')

    @property
    def kv_pair(self):
        return IterEl(data=('k', 'v'))

    def test_element_name(self):
        self.assertEqual('x', self.element.name)

    def test_element_key(self):
        self.assertEqual('x.key', self.element.key)

    def test_element_val(self):
        self.assertEqual('x.val', self.element.val)

    def test_kv_pair_name(self):
        self.assertEqual('<k, v>', self.kv_pair.expr)

    def test_kv_pair_key(self):
        self.assertEqual('k', self.kv_pair.key)

    def test_kv_pair_val(self):
        self.assertEqual('v', self.kv_pair.val)


if __name__ == '__main__':
    unittest.main()
