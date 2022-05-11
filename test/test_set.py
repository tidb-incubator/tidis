import random
import string
import time
import unittest

from rediswrap import RedisWrapper


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

    def random_string(n):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))

    def test_sadd(self):
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        self.assertEqual(self.r.scard(self.k1), 200)
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 0)
        self.assertEqual(self.r.scard(self.k1), 200)

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
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        self.assertEqual(self.r.sadd(self.k1, self.v2), 1)
        self.assertListEqual(self.r.execute_command('smismember', self.k1, self.v1, self.v2, 'not_exist'), [1, 1, 0])

    def test_smembers(self):
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        self.assertSetEqual(self.r.smembers(self.k1), set([str(i) for i in range(200)]))

    def test_srem(self):
        for i in range(200):
            self.assertEqual(self.r.sadd(self.k1, str(i)), 1)
        for i in range(10, 100):
            self.assertEqual(self.r.srem(self.k1, str(i)), 1)
            self.assertEqual(self.r.scard(self.k1), 199 + 10 - i)

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

    def test_pexpire(self):
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('pexpire', self.k1, 5000))
        self.assertLessEqual(self.r.execute_command('pttl', self.k1), 5000)
        self.assertEqual(self.r.scard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.scard(self.k1), 0)

    def test_pexpireat(self):
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        # expire in 5s
        ts = int(round(time.time() * 1000)) + 5000
        self.assertTrue(self.r.execute_command('pexpireat', self.k1, ts))
        self.assertLessEqual(self.r.execute_command('pttl', self.k1), 5000)
        self.assertEqual(self.r.scard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.scard(self.k1), 0)

    def test_expire(self):
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        # expire in 5s
        self.assertTrue(self.r.execute_command('expire', self.k1, 5))
        self.assertLessEqual(self.r.execute_command('ttl', self.k1), 5)
        self.assertEqual(self.r.scard(self.k1), 1)
        time.sleep(6)
        self.assertEqual(self.r.scard(self.k1), 0)

    def test_expireat(self):
        self.assertEqual(self.r.sadd(self.k1, self.v1), 1)
        # expire in 5s
        ts = int(round(time.time())) + 5
        self.assertTrue(self.r.execute_command('expireat', self.k1, ts))
        self.assertLessEqual(self.r.execute_command('ttl', self.k1), 5)
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
