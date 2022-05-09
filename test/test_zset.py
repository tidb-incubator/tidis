import random
import string
import time
import unittest

from rediswrap import RedisWrapper


class ZsetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = RedisWrapper().get_instance()

        cls.k1 = '__zset1__'
        cls.k2 = '__zset2__'

        cls.v1 = 'value1'
        cls.v2 = 'value2'

    def setUp(self):
        self.r.execute_command('del', self.k1)
        self.r.execute_command('del', self.k2)
        pass

    def random_string(n):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))

    def test_zadd(self):
        for i in range(200):
            self.assertEqual(self.r.zadd(self.k1, {str(i): i}), 1)
        self.assertEqual(self.r.zcard(self.k1), 200)
        for i in range(200):
            self.assertEqual(self.r.zadd(self.k1, {str(i): i}), 0)
        self.assertEqual(self.r.zcard(self.k1), 200)
        # test for add multiple member score
        self.assertEqual(self.r.zadd(self.k1, {str(200): 200, str(201): 201}), 2)
        self.assertEqual(self.r.zcard(self.k1), 202)

    def test_zcard(self):
        self.assertEqual(self.r.zcard(self.k1), 0)
        for i in range(200):
            self.assertEqual(self.r.zadd(self.k1, {str(i): i}), 1)
        self.assertEqual(self.r.zcard(self.k1), 200)
        for i in range(200):
            self.assertEqual(self.r.zadd(self.k1, {str(i): i}), 0)
        self.assertEqual(self.r.zcard(self.k1), 200)

    def test_zrange(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, {str(i): i}), 1)
        self.assertListEqual(self.r.zrange(self.k1, 0, -1, False, False), [str(i) for i in range(100)])
        self.assertListEqual(self.r.zrange(self.k1, 10, 20, False, False), [str(i) for i in range(10, 21)])
        self.assertListEqual(self.r.zrange(self.k1, 20, 10, False, False), [])
        # range with scores
        self.assertListEqual(self.r.zrange(self.k1, 10, 20, False, True), [(str(i), i) for i in range(10, 21)])

    def test_zrevrange(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, {str(i): 100 - i}), 1)
        self.assertListEqual(self.r.zrevrange(self.k1, 0, -1, False), [str(i) for i in range(100)])
        self.assertListEqual(self.r.zrevrange(self.k1, 10, 20, False), [str(i) for i in range(10, 20)])
        self.assertListEqual(self.r.zrevrange(self.k1, 20, 10, False), [])
        #  range with scores
        self.assertListEqual(self.r.zrevrange(self.k1, 10, 20, True), [(str(i), 100 - i) for i in range(10, 20)])

    def test_zrangebyscore(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, {str(i): 100 - i}), 1)
        # self.assertListEqual(self.r.zrangebyscore(self.k1, '-inf', '+inf'), [str(i) for i in range(100)])
        self.assertListEqual(self.r.zrangebyscore(self.k1, '0', '-1'), list(reversed([str(i) for i in range(100)])))

    def test_zrevrangebyscore(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, {str(i): 100 - i}), 1)
        # self.assertListEqual(self.r.zrangebyscore(self.k1, '-inf', '+inf'), [str(i) for i in range(100)])
        self.assertListEqual(self.r.zrevrangebyscore(self.k1, '0', '-1'), [str(i) for i in range(100)])

    def test_zremrangebyscore(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, {str(i): i}), 1)
        self.assertEqual(self.r.zremrangebyscore(self.k1, 21, 30), 10)

    def test_zcount(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, {str(i): i}), 1)
        self.assertEqual(self.r.zcount(self.k1, 50, 100), 50)

    def test_zscore(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, {str(i): i}), 1)
        for i in range(100):
            self.assertEqual(self.r.zscore(self.k1, str(i)), i)

    def test_zrem(self):
        for i in range(100):
            self.assertEqual(self.r.zadd(self.k1, {str(i): i}), 1)
        for i in range(10, 100):
            self.assertEqual(self.r.zrem(self.k1, str(i)), 1)
        self.assertEqual(self.r.zcard(self.k1), 10)

    def test_pexpire(self):
        self.assertEqual(self.r.zadd(self.k1, {self.v1: 10}), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('pexpire', self.k1, 5000))
        self.assertLessEqual(self.r.execute_command('pttl', self.k1), 5000)
        self.assertEqual(self.r.zcard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.zcard(self.k1), 0)

    def test_pexpireat(self):
        self.assertEqual(self.r.zadd(self.k1, {self.v1: 10}), 1)
        # expire in 5s
        ts = int(round(time.time() * 1000)) + 5000
        self.assertTrue(self.r.execute_command('pexpireat', self.k1, ts))
        self.assertLessEqual(self.r.execute_command('pttl', self.k1), ts)
        self.assertEqual(self.r.zcard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.zcard(self.k1), 0)

    def test_expire(self):
        self.assertEqual(self.r.zadd(self.k1, {self.v1: 10}), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('expire', self.k1, 5))
        self.assertLessEqual(self.r.execute_command('ttl', self.k1), 5)
        self.assertEqual(self.r.zcard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.zcard(self.k1), 0)

    def test_expireat(self):
        self.assertEqual(self.r.zadd(self.k1, {self.v1: 10}), 1)
        # expire in 5s
        ts = int(round(time.time())) + 5
        self.assertTrue(self.r.execute_command('expireat', self.k1, ts))
        self.assertLessEqual(self.r.execute_command('ttl', self.k1), ts)
        self.assertEqual(self.r.zcard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.zcard(self.k1), 0)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('del', cls.k1)
        cls.r.execute_command('del', cls.k2)
        print('test data cleaned up')
