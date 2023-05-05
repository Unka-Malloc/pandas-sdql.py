import unittest

from pysdql.core.utils.type_checker import (
    is_int,
    is_float,
    is_date,
    is_str,
)

class TestTypeChecker(unittest.TestCase):
    def test_is_float(self):
        self.assertEqual(True, is_float('100.0'))


if __name__ == '__main__':
    unittest.main()
