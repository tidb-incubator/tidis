import random
import time
import unittest

from rediswrap import RedisWrapper
from test_util import sec_ts_after_five_secs, msec_ts_after_five_secs, NOT_EXISTS_LITERAL, CmdType, \
    trigger_async_del_size


class SetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = RedisWrapper.get_instance()

        cls.k1 = '__set1__'
        cls.k2 = '__set2__'
        cls.k3 = '__set3__'

        cls.v1 = 'value1'
        cls.v2 = 'value2'

    def setUp(self):
        self.r.execute_command('del', self.k1)
        self.r.execute_command('del', self.k2)
        self.r.execute_command('del', self.k3)
        pass

    def test_sadd(self):
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        self.assertEqual(self.r.scard(self.k1), 200)
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 0)
        self.assertEqual(self.r.scard(self.k1), 200)

    def test_type(self):
        self.assertEqual(self.r.type(self.k1), CmdType.NULL.value)
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        self.assertEqual(self.r.type(self.k1), CmdType.SET.value)

    def test_scard(self):
        self.assertEqual(self.r.scard(self.k1), 0)
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        self.assertEqual(self.r.scard(self.k1), 200)
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 0)
        self.assertEqual(self.r.scard(self.k1), 200)

    def test_sismember(self):
        for i in range(100):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        for i in range(100):
            self.assertEqual(self.r.sismember(self.k1, str(i)), 1)
        for i in range(100, 200):
            self.assertEqual(self.r.sismember(self.k1, str(i)), 0)

    def test_smismember(self):
        for i in range(1, 100):
            self.assertListEqual(self.r.execute_command('smismember', self.k1, *(str(j) for j in range(i))), [0] * i)

        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        self.assertEqual(self.r.sadd(self.k1, self.v2), 1)
        self.assertListEqual(self.r.execute_command('smismember', self.k1, self.v1, self.v2, NOT_EXISTS_LITERAL),
                             [1, 1, 0])

    def test_smembers(self):
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        self.assertSetEqual(self.r.smembers(self.k1), set([str(i) for i in range(200)]))

    def test_srandmember(self):
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        self.assertIn(self.r.srandmember(self.k1), set([str(i) for i in range(200)]))
        self.assertEqual(len(self.r.srandmember(self.k1, 10)), 10)
        self.assertEqual(len(self.r.srandmember(self.k1, -10)), 10)
        self.assertEqual(len(self.r.srandmember(self.k1, 300)), 200)
        self.assertEqual(len(self.r.srandmember(self.k1, -300)), 300)

    def test_srem(self):
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        for i in range(10, 100):
            self.assertEqual(self.r.srem(self.k1, str(i)), 1)
            self.assertEqual(self.r.scard(self.k1), 199 + 10 - i)

        # multi values
        self.assertEqual(self.r.scard(self.k1), 110)
        self.assertEqual(self.r.srem(self.k1, "1", "2", "3"), 3)
        self.assertEqual(self.r.scard(self.k1), 107)

    def test_spop(self):
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        pop_num = random.randint(1, 199)
        popped_values = self.r.spop(self.k1, pop_num)
        self.assertEqual(len(popped_values), pop_num)
        self.assertEqual(self.r.scard(self.k1), 200 - pop_num)
        for i in range(200):
            v = str(i)
            self.assertEqual(self.r.sismember(self.k1, v), 0 if v in popped_values else 1)

    def test_del(self):
        self.assertTrue(self.r.sadd(self.k1, self.v2), 1)
        self.assertEqual(self.r.scard(self.k1), 1)
        self.assertEqual(self.r.execute_command('del', self.k1), 1)
        self.assertEqual(self.r.scard(self.k1), 0)

        # multi keys
        self.assertTrue(self.r.sadd(self.k2, self.v2))
        self.assertTrue(self.r.sadd(self.k3, self.v2))
        self.assertEqual(self.r.scard(self.k2), 1)
        self.assertEqual(self.r.scard(self.k3), 1)
        self.assertEqual(self.r.execute_command("del", self.k1, self.k2, self.k3), 2)
        self.assertEqual(self.r.scard(self.k2), 0)
        self.assertEqual(self.r.scard(self.k3), 0)

    def test_async_del(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.r.sadd(self.k1, str(i)))
        for i in range(size):
            self.assertTrue(self.r.sismember(self.k1, str(i)))
        self.assertTrue(self.r.delete(self.k1))
        self.assertEqual(self.r.scard(self.k1), 0)
        self.assertTrue(self.r.sadd(self.k1, self.v1))

    def test_async_expire(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.r.sadd(self.k1, str(i)))
        for i in range(size):
            self.assertTrue(self.r.sismember(self.k1, str(i)))
        self.assertTrue(self.r.expire(self.k1, 1))
        time.sleep(1)
        self.assertEqual(self.r.scard(self.k1), 0)

    def test_persist(self):
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('pexpire', self.k1, 5000))
        pttl = self.r.execute_command('pttl', self.k1)
        self.assertLessEqual(pttl, 5000)
        self.assertGreater(pttl, 0)
        self.assertEqual(self.r.scard(self.k1), 1)
        # persis the key
        self.assertEqual(self.r.persist(self.k1), 1)
        self.assertEqual(self.r.execute_command('pttl', self.k1), -1)

    def test_pexpire(self):
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('pexpire', self.k1, 5000))
        pttl = self.r.execute_command('pttl', self.k1)
        self.assertLessEqual(pttl, 5000)
        self.assertGreater(pttl, 0)
        self.assertEqual(self.r.scard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.scard(self.k1), 0)

    def test_pexpireat(self):
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('pexpireat', self.k1, msec_ts_after_five_secs()))
        time.sleep(1)
        pttl = self.r.execute_command('pttl', self.k1)
        self.assertLess(pttl, 5000)
        self.assertGreater(pttl, 0)
        self.assertEqual(self.r.scard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.scard(self.k1), 0)

    def test_expire(self):
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('expire', self.k1, 5))
        ttl = self.r.execute_command('ttl', self.k1)
        self.assertLessEqual(ttl, 5)
        self.assertGreater(ttl, 0)
        self.assertEqual(self.r.scard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.scard(self.k1), 0)

    def test_expireat(self):
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('expireat', self.k1, sec_ts_after_five_secs()))
        ttl = self.r.execute_command('ttl', self.k1)
        self.assertLessEqual(ttl, 5)
        self.assertGreater(ttl, 0)
        self.assertEqual(self.r.scard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.scard(self.k1), 0)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('del', cls.k1)
        cls.r.execute_command('del', cls.k2)
        cls.r.execute_command('del', cls.k3)
        print('test data cleaned up')
