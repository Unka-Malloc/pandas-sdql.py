import unittest

from pysdql.core.killer.SDQLInspector import SDQLInspector

from pysdql.query.tpch import tpch


class MyTestCase(unittest.TestCase):
    def test_findall_bindings(self):
        query_object = tpch.q6().optimize_obj()
        print(SDQLInspector.findall_bindings(query_object))

    def test_replace_cond(self):
        query_object = tpch.q6().optimize_obj()




if __name__ == '__main__':
    unittest.main()
