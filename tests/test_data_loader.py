import unittest

import pysdql


class MyTestCase(unittest.TestCase):
    def test_read_csv(self):
        self.assertEqual(
            'let t1 = load[{<col0: int, col1: string, col2: int, col3: date, col4: real> -> int}]("t1.csv")',
            pysdql.read_csv(r't1.csv').operations.peak().expr)

        self.assertEqual(
            'let t1 = load[{<ID: int, Name: string, Age: int, Birthday: date, Grade: real> -> int}]("t1.csv")',
            pysdql.read_csv(r't1.csv', names=['ID', 'Name', 'Age', 'Birthday', 'Grade']).operations.peak().expr)

        self.assertEqual(
            ['ID', 'Name', 'Age', 'Birthday', 'Grade'],
            list(pysdql.read_csv(r't1.csv', names=['ID', 'Name', 'Age', 'Birthday', 'Grade']).__columns))




if __name__ == '__main__':
    unittest.main()
