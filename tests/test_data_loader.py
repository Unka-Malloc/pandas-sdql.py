import unittest

import pysdql


class MyTestCase(unittest.TestCase):
    def test_read_tbl(self):
        r = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', names=pysdql.LINEITEM_COLS)


if __name__ == '__main__':
    unittest.main()
