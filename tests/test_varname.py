import unittest

import pysdql

class TestVarName(unittest.TestCase):
    def test_name(self):
        df = pysdql.DataFrame()
        self.assertEqual(df.name, 'df')

if __name__ == '__main__':
    unittest.main()
