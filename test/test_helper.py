import unittest

from test_hash import HashTest
from test_list import ListTest
from test_set import SetTest
from test_string import StringTest
from test_zset import ZsetTest

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(StringTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(HashTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ListTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(SetTest))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ZsetTest))

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
