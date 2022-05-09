import time
import unittest

from rediswrap import RedisWrapper


class HashTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = RedisWrapper().get_instance()

        cls.k1 = '__hash1__'
        cls.k2 = '__hash2__'

        cls.f1 = 'f1'
        cls.f2 = 'f2'
        cls.f3 = 'f3'
        cls.f4 = 'f4'

        cls.v1 = 'value1'
        cls.v2 = 'value2'
        cls.v3 = 'value3'
        cls.v4 = 'value4'

    def setUp(self):
        self.r.execute_command('del', self.k1)
        self.r.execute_command('del', self.k2)
        pass

    def test_hget(self):
        self.assertEqual(self.r.hset(self.k1, self.f1, self.v1), 1)
        self.assertEqual(self.v1, self.r.hget(self.k1, self.f1))

    def test_hset(self):
        self.assertEqual(self.r.hset(self.k1, self.f1, self.v1), 1)
        self.assertEqual(self.v1, self.r.hget(self.k1, self.f1))

    def test_hexists(self):
        self.assertEqual(self.r.hset(self.k1, self.f1, self.v1), 1)
        self.assertTrue(self.r.hexists(self.k1, self.f1))

    def test_hstrlen(self):
        self.assertEqual(self.r.hset(self.k1, self.f1, self.v1), 1)
        self.assertEqual(self.r.hstrlen(self.k1, self.f1), len(self.v1))

    def test_hlen(self):
        prefix = '__'
        for i in range(0, 200):
            f = '{}{}'.format(prefix, i)
            self.assertEqual(self.r.hset(self.k2, f, f), 1)
        self.assertEqual(self.r.hlen(self.k2), 200)

    def test_hmget(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        self.assertListEqual(self.r.hmget(self.k1, self.f1, self.f2, self.f3), [self.v1, self.v2, self.v3])

    def test_hdel(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        self.assertEqual(self.r.hdel(self.k1, self.f1, self.f2, self.f3, self.f4), 3)
        self.assertEqual(self.r.hlen(self.k1), 0)

        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        self.assertEqual(self.r.hdel(self.k1, self.f1, self.f2), 2)
        self.assertEqual(self.r.hlen(self.k1), 1)

    def test_hkeys(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        self.assertListEqual(self.r.hkeys(self.k1), [self.f1, self.f2, self.f3])

    def test_hvals(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        self.assertListEqual(self.r.hvals(self.k1), [self.v1, self.v2, self.v3])

    def test_hgetall(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        self.assertDictEqual(self.r.hgetall(self.k1), {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3})

    def test_del(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        self.assertTrue(self.r.execute_command("DEL", self.k1))
        self.assertEqual(self.r.hlen(self.k1), 0)

    def test_pexpire(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        # expire in 5s
        self.assertEqual(self.r.execute_command("PEXPIRE", self.k1, 5000), 1)
        self.assertLessEqual(self.r.execute_command("PTTL", self.k1), 5000)
        self.assertEqual(self.r.hlen(self.k1), 3)
        time.sleep(6)
        self.assertEqual(self.r.hlen(self.k1), 0)

    def test_pexpireat(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        # expire in 5s
        ts = int(round(time.time() * 1000)) + 5000
        self.assertEqual(self.r.execute_command('pexpireat', self.k1, ts), 1)
        self.assertLessEqual(self.r.execute_command('pttl', self.k1), ts)
        self.assertEqual(self.r.hlen(self.k1), 3)
        time.sleep(6)
        self.assertEqual(self.r.hlen(self.k1), 0)

    def test_expire(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        # expire in 5s
        self.assertEqual(self.r.execute_command('expire', self.k1, 5), 1)
        self.assertLessEqual(self.r.execute_command('ttl', self.k1), 5)
        self.assertEqual(self.r.hlen(self.k1), 3)
        time.sleep(6)
        self.assertEqual(self.r.hlen(self.k1), 0)

    def test_expireat(self):
        self.assertTrue(self.r.hmset(self.k1, {self.f1: self.v1, self.f2: self.v2, self.f3: self.v3}))
        # expire in 5s
        ts = int(round(time.time())) + 5
        self.assertEqual(self.r.execute_command('expireat', self.k1, ts), 1)
        self.assertLessEqual(self.r.execute_command('ttl', self.k1), ts)
        self.assertEqual(self.r.hlen(self.k1), 3)
        time.sleep(6)
        self.assertEqual(self.r.hlen(self.k1), 0)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('del', cls.k1)
        cls.r.execute_command('del', cls.k2)
        print('test data cleaned up')
