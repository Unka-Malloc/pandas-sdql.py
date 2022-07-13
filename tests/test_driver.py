import unittest

import pysdql

class MyTestCase(unittest.TestCase):

    @property
    def driver(self):
        return pysdql.driver(f'T:/sdql')

    def test_run(self):
        self.driver.run()


if __name__ == '__main__':
    unittest.main()
