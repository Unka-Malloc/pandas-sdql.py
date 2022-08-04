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

    def test_iter_el(self):
        df = pysdql.DataFrame({})
        df.columns.name = 'r1'
        self.assertEqual('<r1_k, r1_v>', df.iter_expr.iter_el.expr)

        df.columns.name = 'r12'
        self.assertEqual('<r12_k, r12_v>', df.iter_expr.iter_el.expr)

        df.columns.name = 'r_1'
        self.assertEqual('<r1_k, r1_v>', df.iter_expr.iter_el.expr)

        df.columns.name = 'r_a'
        self.assertEqual('<ra_k, ra_v>', df.iter_expr.iter_el.expr)

        df.columns.name = 'r_a_1'
        self.assertEqual('<ra1_k, ra1_v>', df.iter_expr.iter_el.expr)

    def test_iter_expr(self):
        df = pysdql.DataFrame({})
        df.columns.name = 'R'
        self.assertEqual('sum (<r_k, r_v> in R)', df.iter_expr.expr)

        df.columns.name = 'r1'
        self.assertEqual('sum (<r1_k, r1_v> in r1)', df.iter_expr.expr)

        df.columns.name = 'sub_r'
        self.assertEqual('sum (<sr_k, sr_v> in sub_r)', df.iter_expr.expr)

if __name__ == '__main__':
    unittest.main()
