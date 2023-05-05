import unittest

import pysdql


class TestColEl(unittest.TestCase):
    @property
    def df(self):
        return pysdql.DataFrame()


if __name__ == '__main__':
    unittest.main()
