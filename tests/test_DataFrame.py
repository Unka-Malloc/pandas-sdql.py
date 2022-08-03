import unittest

import pysdql


class MyTestCase(unittest.TestCase):
    def test_data(self):
        data = {'A': [1, 2],
                'B': [3, 4]}
        df = pysdql.DataFrame(data)
        self.assertEqual('{<A = 1, B = 3> -> 1, <A = 2, B = 4> -> 1}', df.data.expr)

    def test_columns(self):
        data = {'A': [1, 2],
                'B': [3, 4]}
        df = pysdql.DataFrame(data)

        self.assertEqual("['A', 'B']", str(df.columns))
        self.assertEqual('A', df.columns[0])
        self.assertEqual('B', df.columns[1])
        self.assertEqual(True, 'A' in df.columns)
        self.assertEqual(False, 'C' in df.columns)

        df.columns.name = 'S'
        self.assertEqual('S', df.name)
        self.assertEqual('let S = R', str(df.operations.peak()))

if __name__ == '__main__':
    unittest.main()
