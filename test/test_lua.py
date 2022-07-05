import operator
import random
import time
import unittest
from functools import reduce

from rediswrap import RedisWrapper
from test_util import msec_ts_after_five_secs, sec_ts_after_five_secs, NOT_EXISTS_LITERAL


class LuaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = RedisWrapper.get_instance()

        cls.k1 = '__lua1__'
        cls.k2 = '__lua2__'

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

    def execute_eval(self, cmd, *args):
        arg_str = ", ".join(list(map(lambda arg: "'{}'".format(arg), args)))
        lua_script = "return redis.call('{}', {})".format(cmd, arg_str)
        return self.r.execute_command('eval', lua_script, 0)

    # # ================ string ================
    def test_set_get(self):
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        self.assertEqual(self.execute_eval('get', self.k1), self.v1)

        self.assertIsNone(self.execute_eval('set', self.k1, self.v2, 'nx'))
        self.assertNotEqual(self.execute_eval('get', self.k1), self.v2)
        self.assertEqual(self.execute_eval('get', self.k1), self.v1)

        self.assertTrue(self.execute_eval('set', self.k2, self.v2, 'ex', 5))
        ttl = self.execute_eval('ttl', self.k2)
        self.assertLessEqual(ttl, 5)
        self.assertGreater(ttl, 0)
        self.assertIsNotNone(self.execute_eval('get', self.k2), "ttl = {}s, but the key has expired".format(ttl))
        time.sleep(6)
        self.assertIsNone(self.execute_eval('get', self.k2))

        self.assertTrue(self.execute_eval('set', self.k2, self.v2, 'px', 5000))
        pttl = self.execute_eval('pttl', self.k2)
        self.assertLessEqual(pttl, 5000)
        self.assertGreater(pttl, 0)
        self.assertIsNotNone(self.execute_eval('get', self.k2), "pttl = {}ms, but the key has expired".format(pttl))
        time.sleep(6)
        self.assertIsNone(self.execute_eval('get', self.k2))

    def test_setex(self):
        self.assertTrue(self.execute_eval('setex', self.k1, 5, self.v1))
        ttl = self.execute_eval('ttl', self.k1)
        self.assertLessEqual(ttl, 5)
        self.assertGreater(ttl, 0)
        self.assertIsNotNone(self.execute_eval('get', self.k1), "ttl = {}s, but the key has expired".format(ttl))
        time.sleep(6)
        self.assertIsNone(self.execute_eval('get', self.k1))

    def test_setnx(self):
        self.assertEqual(self.execute_eval('setnx', self.k1, self.v1), 1)
        self.assertEqual(self.execute_eval('get', self.k1), self.v1)
        self.assertEqual(self.execute_eval('setnx', self.k1, self.v2), 0)
        self.assertEqual(self.execute_eval('get', self.k1), self.v1)

    def test_mget_mset(self):
        self.assertTrue(self.execute_eval('mset', self.k1, self.v1, self.k2, self.v2))
        self.assertListEqual(self.execute_eval('mget', self.k1, self.k2), [self.v1, self.v2])

    def test_exists(self):
        self.assertFalse(self.execute_eval('exists', self.k1))
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        self.assertTrue(self.execute_eval('exists', self.k1))

        self.assertFalse(self.execute_eval('exists', self.k2))
        self.assertTrue(self.execute_eval('set', self.k2, self.v2))
        self.assertEqual(self.execute_eval('exists', self.k1, self.k2, NOT_EXISTS_LITERAL), 2)

    def test_incr(self):
        self.assertEqual(self.execute_eval("incr", self.k1), 1)
        self.assertEqual(self.execute_eval("incr", self.k1), 2)

        self.assertTrue(self.execute_eval('set', self.k2, self.v2))
        with self.assertRaises(Exception) as cm:
            self.execute_eval("incr", self.k2)
        err = cm.exception
        self.assertEqual(str(err), 'value is not an integer or out of range')

    def test_incrby(self):
        self.assertEqual(self.execute_eval('incrby', self.k1, 1), 1)
        self.assertEqual(self.execute_eval('incrby', self.k1, 9), 10)
        self.assertEqual(self.execute_eval('incrby', self.k1, -15), -5)

        # incr a invalid number
        self.assertTrue(self.execute_eval('set', self.k2, self.v2))

        with self.assertRaises(Exception) as cm:
            self.assertEqual(self.execute_eval('incrby', self.k2, 1), 1)
        err = cm.exception
        self.assertEqual(str(err), 'value is not an integer or out of range')

    def test_decr(self):
        self.assertEqual(self.execute_eval("decr", self.k1), -1)
        self.assertEqual(self.execute_eval("decr", self.k1), -2)

        self.assertTrue(self.execute_eval('set', self.k2, self.v2))
        with self.assertRaises(Exception) as cm:
            self.execute_eval("decr", self.k2)
        err = cm.exception
        self.assertEqual(str(err), 'value is not an integer or out of range')

    def test_decrby(self):
        self.assertEqual(self.execute_eval('decrby', self.k1, 1), -1)
        self.assertEqual(self.execute_eval('decrby', self.k1, 9), -10)
        self.assertEqual(self.execute_eval('decrby', self.k1, -15), 5)

        # incr a invalid number
        self.assertTrue(self.execute_eval('set', self.k2, self.v2))

        with self.assertRaises(Exception) as cm:
            self.assertEqual(self.execute_eval('decrby', self.k2, 1), 1)
        err = cm.exception
        self.assertEqual(str(err), 'value is not an integer or out of range')

    def test_strlen(self):
        self.assertEqual(self.execute_eval('strlen', self.k1), 0)
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        self.assertEqual(self.execute_eval('strlen', self.k1), len(self.v1))

    def test_del(self):
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        v1 = self.execute_eval('get', self.k1)
        self.assertEqual(self.v1, v1)
        v1 = self.execute_eval('del', self.k1)
        self.assertEqual(v1, 1)
        v1 = self.execute_eval('del', self.k1)
        self.assertEqual(v1, 0)
        v1 = self.execute_eval('get', self.k1)
        self.assertIsNone(v1)

        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        self.assertTrue(self.execute_eval('set', self.k2, self.v2))
        self.assertTrue(self.execute_eval('del', self.k1, self.k2))
        self.assertIsNone(self.execute_eval('get', self.k1))
        self.assertIsNone(self.execute_eval('get', self.k2))

    # ================ hash ================
    def test_hget_hset(self):
        self.assertEqual(self.execute_eval('hset', self.k1, self.f1, self.v1), 1)
        self.assertEqual(self.v1, self.execute_eval('hget', self.k1, self.f1))

    def test_hmget_hmset(self):
        self.assertTrue(self.execute_eval('hmset', self.k1, self.f1, self.v1, self.f2, self.v2, self.f3, self.v3))
        self.assertListEqual(self.execute_eval('hmget', self.k1, self.f1, self.f2, self.f3),
                             [self.v1, self.v2, self.v3])

    def test_hexists(self):
        self.assertFalse(self.execute_eval('hexists', self.k1, self.f1))
        self.assertTrue(self.execute_eval('hset', self.k1, self.f1, self.v1), 1)
        self.assertTrue(self.execute_eval('hexists', self.k1, self.f1))

    def test_hstrlen(self):
        self.assertTrue(self.execute_eval('hset', self.k1, self.f1, self.v1), 1)
        self.assertEqual(self.execute_eval('hstrlen', self.k1, self.f1), len(self.v1))

    def test_hlen(self):
        prefix = '__'
        for i in range(0, 200):
            f = '{}{}'.format(prefix, i)
            self.assertEqual(self.execute_eval('hset', self.k2, f, f), 1)
        self.assertEqual(self.execute_eval('hlen', self.k2), 200)

    def test_hkeys(self):
        self.assertTrue(self.execute_eval('hmset', self.k1, self.f1, self.v1, self.f2, self.v2, self.f3, self.v3))
        self.assertListEqual(self.execute_eval('hkeys', self.k1), [self.f1, self.f2, self.f3])

        self.assertListEqual(self.execute_eval('hkeys', self.k2), [])

    def test_hvals(self):
        self.assertTrue(self.execute_eval('hmset', self.k1, self.f1, self.v1, self.f2, self.v2, self.f3, self.v3))
        self.assertListEqual(self.execute_eval('hvals', self.k1), [self.v1, self.v2, self.v3])

        self.assertListEqual(self.execute_eval('hvals', self.k2), [])

    def test_hgetall(self):
        self.assertTrue(self.execute_eval('hmset', self.k1, self.f1, self.v1, self.f2, self.v2, self.f3, self.v3))
        self.assertListEqual(self.execute_eval('hgetall', self.k1),
                             [self.f1, self.v1, self.f2, self.v2, self.f3, self.v3])

        self.assertListEqual(self.execute_eval('hgetall', self.k2), [])

    def test_hincrby(self):
        self.assertEqual(self.execute_eval('hincrby', self.k1, self.v1, 1), 1)
        self.assertEqual(self.execute_eval('hincrby', self.k1, self.v1, 9), 10)
        self.assertEqual(self.execute_eval('hincrby', self.k1, self.v1, -15), -5)

    def test_hdel(self):
        self.assertTrue(self.execute_eval('hmset', self.k1, self.f1, self.v1, self.f2, self.v2, self.f3, self.v3))
        self.assertEqual(self.execute_eval('hdel', self.k1, self.f1, self.f2, self.f3, self.f4), 3)
        self.assertEqual(self.execute_eval('hlen', self.k1), 0)

        self.assertTrue(self.execute_eval('hmset', self.k1, self.f1, self.v1, self.f2, self.v2, self.f3, self.v3))
        self.assertEqual(self.execute_eval('hdel', self.k1, self.f1, self.f2), 2)
        self.assertEqual(self.execute_eval('hlen', self.k1), 1)

    # ================ list ================
    def test_lpop(self):
        for i in range(200):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.execute_eval('lpop', self.k1), str(i))

    def test_lpush(self):
        for i in range(200):
            self.assertTrue(self.execute_eval('lpush', self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.execute_eval('rpop', self.k1), str(i))

    def test_rpop(self):
        for i in range(200):
            self.assertTrue(self.execute_eval('lpush', self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.execute_eval('rpop', self.k1), str(i))

    def test_rpush(self):
        for i in range(200):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.execute_eval('lpop', self.k1), str(i))

    def test_llen(self):
        for i in range(200):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        self.assertEqual(self.execute_eval('llen', self.k1), 200)

    def test_lindex(self):
        for i in range(200):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        for i in range(200):
            self.assertEqual(self.execute_eval('lindex', self.k1, i), str(i))

    def test_lrange(self):
        for i in range(200):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        self.assertListEqual(self.execute_eval('lrange', self.k1, 10, 100), [str(i) for i in range(10, 101)])

        self.assertListEqual(self.execute_eval('lrange', self.k2, 0, 100), [])

    def test_lset(self):
        with self.assertRaises(Exception) as cm:
            self.execute_eval('lset', self.k1, 0, self.v1)
        err = cm.exception
        self.assertEqual(str(err), 'no such key')
        for i in range(200):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        self.assertTrue(self.execute_eval('lset', self.k1, 100, 'hello'))
        self.assertEqual(self.execute_eval('lindex', self.k1, 100), 'hello')

    def test_ltrim(self):
        for i in range(200):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        self.assertTrue(self.execute_eval('ltrim', self.k1, 0, 99))
        self.assertEqual(100, self.execute_eval('llen', self.k1))
        self.assertListEqual([str(i) for i in range(0, 100)], self.execute_eval('lrange', self.k1, 0, -1))

    def test_lrem(self):
        for i in range(50):
            for j in range(i):
                self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        self.assertEqual(1225, self.execute_eval('llen', self.k1))
        # remove all the same elements
        self.assertEqual(self.execute_eval('lrem', self.k1, 0, 10), 10)
        # remove same elements at most 5 times
        self.assertEqual(self.execute_eval('lrem', self.k1, 5, 11), 5)
        # remove not exists elements
        self.assertEqual(self.execute_eval('lrem', self.k1, 0, 100), 0)

    def test_linsert(self):
        for i in range(100):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        llen = self.execute_eval('llen', self.k1)
        # test insert before the first element
        self.assertEqual(self.execute_eval('linsert', self.k1, 'before', '0', 'hello1'), llen+1)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1), ['hello1'] + [str(i) for i in range(0, 100)])
        # test insert after the first element
        self.assertEqual(self.execute_eval('linsert', self.k1, 'after', 'hello1', 'hello2'), llen+2)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1), ['hello1', 'hello2'] + [str(i) for i in range(0, 100)])
        # test insert in the middle
        self.assertEqual(self.execute_eval('linsert', self.k1, 'before', '50', 'hello3'), llen+3)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0 ,-1), ['hello1', 'hello2'] + [str(i) for i in range(0, 50)] + ['hello3'] + [str(i) for i in range(50, 100)])
        self.assertEqual(self.execute_eval('linsert', self.k1, 'after', '50', 'hello4'), llen+4)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1), ['hello1', 'hello2'] + [str(i) for i in range(0, 50)] + ['hello3', '50', 'hello4'] + [str(i) for i in range(51, 100)])
        # test insert before the last element
        self.assertEqual(self.execute_eval('linsert', self.k1, 'before', '99', 'hello5'), llen+5)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1), ['hello1', 'hello2'] + [str(i) for i in range(0, 50)] + ['hello3', '50', 'hello4'] + [str(i) for i in range(51, 99)] + ['hello5', '99'])
        # test insert after the last element
        self.assertEqual(self.execute_eval('linsert', self.k1, 'after', '99', 'hello6'), llen+6)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1), ['hello1', 'hello2'] + [str(i) for i in range(0, 50)] + ['hello3', '50', 'hello4'] + [str(i) for i in range(51, 99)] + ['hello5', '99', 'hello6'])


    # ================ set ================
    def test_sadd(self):
        for i in range(200):
            self.assertEqual(self.execute_eval('sadd', self.k1, str(i)), 1)
        self.assertEqual(self.execute_eval('scard', self.k1), 200)
        for i in range(200):
            self.assertEqual(self.execute_eval('sadd', self.k1, str(i)), 0)
        self.assertEqual(self.execute_eval('scard', self.k1), 200)

    def test_scard(self):
        self.assertEqual(self.execute_eval('scard', self.k1), 0)
        for i in range(200):
            self.assertEqual(self.execute_eval('sadd', self.k1, str(i)), 1)
        self.assertEqual(self.execute_eval('scard', self.k1), 200)
        for i in range(200):
            self.assertEqual(self.execute_eval('sadd', self.k1, str(i)), 0)
        self.assertEqual(self.execute_eval('scard', self.k1), 200)

    def test_sismember(self):
        for i in range(100):
            self.assertEqual(self.execute_eval('sadd', self.k1, str(i)), 1)
        for i in range(100):
            self.assertEqual(self.execute_eval('sismember', self.k1, str(i)), 1)
        for i in range(100, 200):
            self.assertEqual(self.execute_eval('sismember', self.k1, str(i)), 0)

    def test_smismember(self):
        for i in range(1, 100):
            self.assertListEqual(self.execute_eval('smismember', self.k1, *(str(j) for j in range(i))), [0] * i)

        self.assertEqual(self.execute_eval('sadd', self.k1, self.v1), 1)
        self.assertEqual(self.execute_eval('sadd', self.k1, self.v2), 1)
        self.assertListEqual(self.execute_eval('smismember', self.k1, self.v1, self.v2, NOT_EXISTS_LITERAL), [1, 1, 0])

    def test_smembers(self):
        for i in range(200):
            self.assertEqual(self.execute_eval('sadd', self.k1, str(i)), 1)
        self.assertSetEqual(set(self.execute_eval('smembers', self.k1)), set([str(i) for i in range(200)]))

    def test_srandmember(self):
        for i in range(200):
            self.assertEqual(self.execute_eval('sadd', self.k1, str(i)), 1)
        self.assertIn(self.execute_eval('srandmember', self.k1), set([str(i) for i in range(200)]))
        self.assertEqual(len(self.execute_eval('srandmember', self.k1, 10)), 10)
        self.assertEqual(len(self.execute_eval('srandmember', self.k1, -10)), 10)
        self.assertEqual(len(self.execute_eval('srandmember', self.k1, 300)), 200)
        self.assertEqual(len(self.execute_eval('srandmember', self.k1, -300)), 300)

    def test_srem(self):
        for i in range(200):
            self.assertEqual(self.execute_eval('sadd', self.k1, str(i)), 1)
        for i in range(10, 100):
            self.assertEqual(self.execute_eval('srem', self.k1, str(i)), 1)
            self.assertEqual(self.execute_eval('scard', self.k1), 199 + 10 - i)

        # multi values
        self.assertEqual(self.execute_eval('scard', self.k1), 110)
        self.assertEqual(self.execute_eval('srem', self.k1, "1", "2", "3"), 3)
        self.assertEqual(self.execute_eval('scard', self.k1), 107)

    def test_spop(self):
        for i in range(200):
            self.assertEqual(self.execute_eval('sadd', self.k1, str(i)), 1)
        pop_num = random.randint(1, 199)
        popped_values = self.execute_eval('spop', self.k1, pop_num)
        self.assertEqual(len(popped_values), pop_num)
        self.assertEqual(self.execute_eval('scard', self.k1), 200 - pop_num)
        for i in range(200):
            v = str(i)
            self.assertEqual(self.execute_eval('sismember', self.k1, v), 0 if v in popped_values else 1)

    # ================ sorted set ================
    def test_zadd(self):
        for i in range(200):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 1)
        self.assertEqual(self.execute_eval('zcard', self.k1), 200)
        for i in range(200):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 0)
        self.assertEqual(self.execute_eval('zcard', self.k1), 200)

        # test for add multiple member score
        self.assertEqual(self.execute_eval('zadd', self.k1, 200, str(200), 201, str(201)), 2)
        self.assertEqual(self.execute_eval('zcard', self.k1), 202)

        # zadd xx
        self.assertEqual(self.execute_eval('zcard', self.k2), 0)
        self.assertEqual(self.execute_eval('zadd', self.k2, 'xx', 1, self.v1), 0)
        self.assertEqual(self.execute_eval('zcard', self.k2), 0)
        self.assertEqual(self.execute_eval('zadd', self.k2, 1, self.v1), 1)
        self.assertListEqual(self.execute_eval('zrange', self.k2, 0, -1, 'withscores'), [self.v1, '1'])
        self.assertEqual(self.execute_eval('zadd', self.k2, 'xx', 2, self.v1), 0)
        self.assertEqual(self.execute_eval('zadd', self.k2, 'xx', 'ch', 3, self.v1), 1)
        self.assertListEqual(self.execute_eval('zrange', self.k2, 0, -1, "withscores"), [self.v1, '3'])

        # zadd nx
        self.assertEqual(self.execute_eval('zadd', self.k2, 'nx', 4, self.v1), 0)
        self.assertListEqual(self.execute_eval('zrange', self.k2, 0, -1, "withscores"), [self.v1, '3'])
        self.assertEqual(self.execute_eval('zadd', self.k2, 'nx', 1, self.v2), 1)
        self.assertListEqual(self.execute_eval('zrange', self.k2, 0, -1, "withscores"), [self.v2, '1', self.v1, '3'])

        # zadd ch
        self.assertEqual(self.execute_eval('zadd', self.k2, 1, self.v1, 1, self.v2, 2, 'new_ele'), 1)
        self.assertEqual(self.execute_eval('zadd', self.k2, 'ch', 2, self.v1, 2, self.v2, 2, 'new_ele'), 2)

        # zadd lt, gt, incr are pending

    def test_zcard(self):
        self.assertEqual(self.execute_eval('zcard', self.k1), 0)
        for i in range(200):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 1)
        self.assertEqual(self.execute_eval('zcard', self.k1), 200)
        for i in range(200):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 0)
        self.assertEqual(self.execute_eval('zcard', self.k1), 200)

    def test_zrange(self):
        for i in range(100):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 1)
        self.assertListEqual(self.execute_eval('zrange', self.k1, 0, -1), [str(i) for i in range(100)])
        self.assertListEqual(self.execute_eval('zrange', self.k1, 10, 20),
                             [str(i) for i in range(10, 21)])
        self.assertListEqual(self.execute_eval('zrange', self.k1, 20, 10), [])
        # range with scores
        self.assertListEqual(self.execute_eval('zrange', self.k1, 10, 20, 'withscores'),
                             reduce(operator.add, [[str(i), str(i)] for i in range(10, 21)]))

    def test_zrevrange(self):
        for i in range(100):
            self.assertEqual(self.execute_eval('zadd', self.k1, 100 - i, str(i)), 1)
        self.assertListEqual(self.execute_eval('zrevrange', self.k1, 0, -1), [str(i) for i in range(100)])
        self.assertListEqual(self.execute_eval('zrevrange', self.k1, 10, 20), [str(i) for i in range(10, 21)])
        self.assertListEqual(self.execute_eval('zrevrange', self.k1, 20, 10), [])
        #  range with scores
        self.assertListEqual(self.execute_eval('zrevrange', self.k1, 10, 20, 'withscores'),
                             reduce(operator.add, [[str(i), str(100 - i)] for i in range(10, 21)]))

    def test_zrangebyscore(self):
        for i in range(100):
            self.assertEqual(self.execute_eval('zadd', self.k1, 100 - i, str(i)), 1)
        self.assertListEqual(self.execute_eval('zrangebyscore', self.k1, 0, -1), [])
        self.assertListEqual(self.execute_eval('zrangebyscore', self.k1, '-inf', '+inf'),
                             list(reversed([str(i) for i in range(100)])))

    def test_zrevrangebyscore(self):
        for i in range(100):
            self.assertEqual(self.execute_eval('zadd', self.k1, 100 - i, str(i)), 1)
        self.assertListEqual(self.execute_eval('zrevrangebyscore', self.k1, '+inf', '-inf'),
                             [str(i) for i in range(100)])
        self.assertListEqual(self.execute_eval('zrevrangebyscore', self.k1, -1, 0), [])

    def test_zremrangebyscore(self):
        for i in range(100):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 1)
        self.assertEqual(self.execute_eval('zremrangebyscore', self.k1, 21, 30), 10)

    def test_zcount(self):
        for i in range(100):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 1)
        self.assertEqual(self.execute_eval('zcount', self.k1, 50, 100), 50)

    def test_zscore(self):
        self.assertIsNone(self.execute_eval('zscore', self.k1, self.v1))
        for i in range(100):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 1)
        for i in range(100):
            self.assertEqual(self.execute_eval('zscore', self.k1, str(i)), str(i))

    def test_zrem(self):
        for i in range(100):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 1)
        for i in range(10, 100):
            self.assertEqual(self.execute_eval('zrem', self.k1, str(i)), 1)
        self.assertEqual(self.execute_eval('zcard', self.k1), 10)

        # multi values
        self.assertEqual(self.execute_eval('zcard', self.k1), 10)
        self.assertEqual(self.execute_eval('zrem', self.k1, "1", "2", "3"), 3)
        self.assertEqual(self.execute_eval('zcard', self.k1), 7)

    def test_zrank(self):
        for i in range(100):
            self.assertEqual(self.execute_eval('zadd', self.k1, i, str(i)), 1)
        for i in range(100):
            self.assertEqual(self.execute_eval('zrank', self.k1, str(i)), i)

    def test_zpopmin(self):
        self.assertEqual(self.execute_eval('zadd', self.k1, 1, self.v1, 2, self.v2), 2)
        self.assertListEqual(self.execute_eval('zpopmin', self.k1), [self.v1, '1'])

    def test_zpopmax(self):
        self.assertEqual(self.execute_eval('zadd', self.k1, 1, self.v1, 2, self.v2), 2)
        self.assertListEqual(self.execute_eval('zpopmax', self.k1), [self.v2, '2'])

    def test_zincrby(self):
        self.assertEqual(self.execute_eval('zadd', self.k1, 1, self.v1, 2, self.v2), 2)
        self.assertListEqual(self.execute_eval('zrange', self.k1, 0, -1, 'withscores'), [self.v1, '1', self.v2, '2'])
        self.assertEqual(self.execute_eval('zincrby', self.k1, 2, self.v1), '3')
        self.assertListEqual(self.execute_eval('zrange', self.k1, 0, -1, 'withscores'), [self.v2, '2', self.v1, '3'])
        self.assertEqual(self.execute_eval('zscore', self.k1, self.v1), '3')

        self.assertEqual(self.execute_eval('zincrby', self.k1, -1.2, self.v1), '1.8')
        self.assertListEqual(self.execute_eval('zrange', self.k1, 0, -1, 'withscores'), [self.v1, '1.8', self.v2, '2'])
        self.assertEqual(self.execute_eval('zscore', self.k1, self.v1), '1.8')

        self.assertEqual(self.execute_eval('zincrby', self.k1, 1.5, NOT_EXISTS_LITERAL), '1.5')
        self.assertListEqual(self.execute_eval('zrange', self.k1, 0, -1, 'withscores'),
                             [NOT_EXISTS_LITERAL, '1.5', self.v1, '1.8', self.v2, '2'])
        self.assertEqual(self.execute_eval('zscore', self.k1, NOT_EXISTS_LITERAL), '1.5')

    # ================ generic ================

    def test_persist(self):
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.execute_eval('pexpire', self.k1, 5000))
        pttl = self.execute_eval('pttl', self.k1)
        self.assertLessEqual(pttl, 5000)
        self.assertGreater(pttl, 0)
        self.assertEqual(self.execute_eval('get', self.k1), self.v1)
        # persis the key
        self.assertEqual(self.execute_eval('persist', self.k1), 1)
        self.assertEqual(self.execute_eval('pttl', self.k1), -1)

    def test_pexpire(self):
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.execute_eval('pexpire', self.k1, 5000))
        pttl = self.execute_eval('pttl', self.k1)
        self.assertLessEqual(pttl, 5000)
        self.assertGreater(pttl, 0)
        self.assertEqual(self.execute_eval('get', self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.execute_eval('get', self.k1))

    def test_pexpireat(self):
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.execute_eval('pexpireat', self.k1, msec_ts_after_five_secs()))
        time.sleep(1)
        pttl = self.execute_eval('pttl', self.k1)
        self.assertLessEqual(pttl, 5000)
        self.assertGreater(pttl, 0)
        self.assertEqual(self.execute_eval('get', self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.execute_eval('get', self.k1))

    def test_expire(self):
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.execute_eval('expire', self.k1, 5))
        ttl = self.execute_eval('ttl', self.k1)
        self.assertLessEqual(ttl, 5)
        self.assertGreater(ttl, 0)
        self.assertEqual(self.execute_eval('get', self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.execute_eval('get', self.k1))

    def test_expireat(self):
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))
        # expire in 5s
        self.assertTrue(self.execute_eval('expireat', self.k1, sec_ts_after_five_secs()))
        ttl = self.execute_eval('ttl', self.k1)
        self.assertLessEqual(ttl, 5)
        self.assertGreater(ttl, 0)
        self.assertEqual(self.execute_eval('get', self.k1), self.v1)
        time.sleep(6)
        self.assertIsNone(self.execute_eval('get', self.k1))

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('del', cls.k1)
        cls.r.execute_command('del', cls.k2)
        print('test data cleaned up')
