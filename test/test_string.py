import time
import unittest

from rediswrap import RedisWrapper


class StringTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = RedisWrapper().get_instance()
        cls.k1 = '__string1__'
        cls.v1 = 'value1'
        cls.k2 = '__string2__'
        cls.v2 = 'value2'

    def setUp(self):
        self.r.delete(self.k1)
        self.r.delete(self.k2)
        pass

    def test_get(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        v1 = self.r.get(self.k1)
        self.assertEqual(self.v1, v1, '{} != {}'.format(v1, self.v1))

    def test_set(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        v1 = self.r.get(self.k1)
        self.assertEqual(self.v1, v1, '{} != {}'.format(v1, self.v1))

    def test_set_expire(self):
        self.assertTrue(self.r.set(self.k2, self.v2, px=5000))
        v2 = self.r.get(self.k2)
        self.assertEqual(self.v2, v2, '{} != {}'.format(v2, self.v2))

        self.assertTrue(self.r.set(self.k2, self.v1, ex=5))
        v1 = self.r.get(self.k2)
        self.assertEqual(self.v1, v1, '{} != {}'.format(v1, self.v1))

    def test_del(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        v1 = self.r.get(self.k1)
        self.assertEqual(self.v1, v1, '{} != {}'.format(v1, self.v1))
        v1 = self.r.delete(self.k1)
        self.assertEqual(v1, 1, '{} != 1'.format(v1))
        v1 = self.r.delete(self.k1)
        self.assertEqual(v1, 0, '{} != 0'.format(v1))
        v1 = self.r.get(self.k1)
        self.assertIsNone(v1, '{} != None'.format(v1))

    def test_mget(self):
        self.assertTrue(self.r.mset({self.k1: self.v1, self.k2: self.v2}))
        self.assertListEqual(self.r.mget(self.k1, self.k2), [self.v1, self.v2])

    def test_mset(self):
        self.assertTrue(self.r.mset({self.k1: self.v1, self.k2: self.v2}))
        self.assertListEqual(self.r.mget(self.k1, self.k2), [self.v1, self.v2])

    def test_incr(self):
        # incr a new key
        self.assertEqual(self.r.execute_command("INCR", self.k1), 1)
        # incr a valid number key
        self.assertEqual(self.r.execute_command("INCR", self.k1), 2)

        # incr a invalid number
        self.assertTrue(self.r.set(self.k2, self.v2))

        with self.assertRaises(Exception) as cm:
            self.r.execute_command("INCR", self.k2)
        err = cm.exception
        self.assertEqual(str(err), 'invalid digit found in string')

    def test_decr(self):
        # decr a new key
        self.assertEqual(self.r.execute_command("DECR", self.k1), -1)
        # decr a valid number key
        self.assertEqual(self.r.execute_command("DECR", self.k1), -2)

        # decr a invalid number
        self.assertTrue(self.r.set(self.k2, self.v2))

        with self.assertRaises(Exception) as cm:
            self.r.execute_command("DECR", self.k2)
        err = cm.exception
        self.assertEqual(str(err), 'invalid digit found in string')

    def test_pexpire(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.r.pexpire(self.k1, 5000))
        self.assertLessEqual(self.r.pttl(self.k1), 5000)
        self.assertEqual(self.r.get(self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.r.get(self.k1))

    def test_pexpireat(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        # expire in 5s
        ts = int(round(time.time() * 1000)) + 5000
        self.assertTrue(self.r.pexpireat(self.k1, ts))
        self.assertLessEqual(self.r.pttl(self.k1), ts)
        self.assertEqual(self.r.get(self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.r.get(self.k1))

    def test_expire(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.r.expire(self.k1, 5))
        self.assertLessEqual(self.r.ttl(self.k1), 5)
        self.assertEqual(self.r.get(self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.r.get(self.k1))

    def test_expireat(self):
        self.assertTrue(self.r.set(self.k1, self.v1))
        # expire in 5s
        ts = int(round(time.time())) + 5
        self.assertTrue(self.r.expireat(self.k1, ts))
        self.assertLessEqual(self.r.ttl(self.k1), 5)
        self.assertEqual(self.r.get(self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.r.get(self.k1))

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.delete(cls.k1)
        cls.r.delete(cls.k2)
        print('test data cleaned up')
