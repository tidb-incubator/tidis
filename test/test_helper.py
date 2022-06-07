import sys
import unittest

from test_generic import GenericTest
from test_hash import HashTest
from test_list import ListTest
from test_lua import LuaTest
from test_set import SetTest
from test_string import StringTest
from test_zset import ZsetTest

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(GenericTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(StringTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(HashTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ListTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(SetTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ZsetTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(LuaTest))

    runner = unittest.TextTestRunner(verbosity=2)
    errors = runner.run(suite).errors
    if len(errors) > 0:
        sys.exit(1)
