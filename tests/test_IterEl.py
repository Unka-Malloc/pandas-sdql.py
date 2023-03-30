import unittest
from pysdql.core.exprs.basic.IterEl import IterEl


class MyTestCase(unittest.TestCase):
    @property
    def element(self):
        return IterEl(data='x')

    @property
    def kv_pair(self):
        return IterEl(data=('r_k', 'r_v'))

    def test_element_key(self):
        self.assertEqual('x_k', self.element.key)

    def test_element_val(self):
        self.assertEqual('x_v', self.element.val)

    def test_kv_pair_key(self):
        self.assertEqual('k', self.kv_pair.key)

    def test_kv_pair_val(self):
        self.assertEqual('v', self.kv_pair.val)

    def test_rename(self):
        el = IterEl(data='x')
        el.rename('r')
        print(el.expr)
        # self.assertEqual('r_k', self.element.key)
        # self.assertEqual('r_v', self.element.val)



if __name__ == '__main__':
    unittest.main()
