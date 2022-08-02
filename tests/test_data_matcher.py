import unittest

from pysdql.core.util.data_matcher import (
    match_int,
    match_real,
    match_bool,
    match_string,
    match_date,
    match_record,
    match_dict,
    from_scalar,
    from_record,
    from_dict,
    from_expr,
)


class MyTestCase(unittest.TestCase):
    def test_match_int(self):
        self.assertEqual(True, match_int('0'))
        self.assertEqual(True, match_int('+1'))
        self.assertEqual(True, match_int('-1'))
        self.assertEqual(False, match_int('0.0'))
        self.assertEqual(False, match_int('+0.0'))
        self.assertEqual(False, match_int('-0.0'))

    def test_match_real(self):
        self.assertEqual(True, match_real('0.01'))
        self.assertEqual(True, match_real('+0.01'))
        self.assertEqual(True, match_real('-0.01'))
        self.assertEqual(True, match_real('1.2E9'))
        self.assertEqual(True, match_real('+1.2E9'))
        self.assertEqual(True, match_real('-1.2E9'))
        self.assertEqual(True, match_real('1.2E-9'))
        self.assertEqual(True, match_real('+1.2E-9'))
        self.assertEqual(True, match_real('-1.2E-9'))
        self.assertEqual(True, match_real('0.01E9'))
        self.assertEqual(True, match_real('+0.01E9'))
        self.assertEqual(True, match_real('-0.01E9'))
        self.assertEqual(True, match_real('0.01E-9'))
        self.assertEqual(True, match_real('+0.01E-9'))
        self.assertEqual(True, match_real('-0.01E-9'))

    def test_match_bool(self):
        self.assertEqual(True, match_bool('true'))
        self.assertEqual(True, match_bool('false'))
        self.assertEqual(False, match_bool('truefalse'))
        self.assertEqual(False, match_bool('falsetrue'))

    def test_match_string(self):
        self.assertEqual(True, match_string('""'))
        self.assertEqual(True, match_string('"any"'))
        self.assertEqual(True, match_string('"Hello, World"'))
        self.assertEqual(True, match_string('"."'))
        self.assertEqual(True, match_string(r'"+-*/\!@#$%^&(){}[]|~`?<>,.:;"'))
        self.assertEqual(False, match_string('any'))
        self.assertEqual(True, match_string('"true"'))
        self.assertEqual(True, match_string('"false"'))

    def test_match_record(self):
        self.assertEqual(True, match_record('<>'))
        self.assertEqual(True, match_record('<a=1>'))
        self.assertEqual(True, match_record('<a=1,b=2,c=3>'))
        self.assertEqual(True, match_record('<a=1, b=2, c=3>'))
        self.assertEqual(True, match_record('<a = 1, b = 2, c = 3>'))
        self.assertEqual(True, match_record('<a = "Apple", b = "Banana">'))
        self.assertEqual(True, match_record('<a_fruit = "Apple", b_fruit = "Banana">'))

    def test_match_dict(self):
        self.assertEqual(True, match_dict('{}'))
        self.assertEqual(True, match_dict('{"a"->"Apple"}'))
        self.assertEqual(True, match_dict('{"a" -> "Apple", "b" -> "Banana"}'))
        self.assertEqual(True, match_dict('{<a = 1, b = 2, c = 3> -> 1}'))
        self.assertEqual(True, match_dict('{<a = "Apple", b = "Banana"> -> 1}'))

    def test_match_date(self):
        self.assertEqual(True, match_date('DateValue(20220801)'))

    def test_match_scalar(self):
        self.assertEqual(0, from_scalar('0'))
        self.assertEqual(0.1, from_scalar('0.1'))
        self.assertEqual('Hello, World', from_scalar('"Hello, World"'))
        self.assertEqual('ID', from_scalar('ID'))
        self.assertEqual('2022-08-01', from_scalar('DateValue(20220801)'))

    def test_match_expr(self):
        self.assertEqual('<a = 1, b = 2, c = 3>', from_expr('<a = 1, b = 2, c = 3>').expr)
        self.assertEqual('<a = "Apple", b = "Banana">', from_expr('<a = "Apple", b = "Banana">').expr)
        self.assertEqual('{<a = "Apple", b = "Banana"> -> 1}', from_expr('{<a = "Apple", b = "Banana"> -> 1}').expr)

    def test_from_record(self):
        self.assertEqual('<a = 1, b = 2, c = 3>', from_record('<a = 1, b = 2, c = 3>').expr)

    def test_from_dict(self):
        self.assertEqual('{<a = "Apple", b = "Banana"> -> 1}', from_dict('{<a = "Apple", b = "Banana"> -> 1}').expr)


if __name__ == '__main__':
    unittest.main()
