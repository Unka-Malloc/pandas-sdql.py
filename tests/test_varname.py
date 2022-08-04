import unittest

import pysdql

class MyTestCase(unittest.TestCase):
    def test_something(self):
        df = pysdql.DataFrame()
        print(df.sdql_expr)




if __name__ == '__main__':
    unittest.main()
