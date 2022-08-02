import unittest

import pysdql


class MyTestCase(unittest.TestCase):
    def test_data(self):
        data = {'A': [1, 2],
                'B': [3, 4]}
        df = pysdql.DataFrame(data)
        self.assertEqual('{<A = 1, B = 3> -> 1, <A = 2, B = 4> -> 1}', df.data.expr)


if __name__ == '__main__':
    unittest.main()
