import operator
import random
import time
import unittest
import redis
from functools import reduce

from rediswrap import RedisWrapper
from test_util import msec_ts_after_five_secs, sec_ts_after_five_secs, NOT_EXISTS_LITERAL, CmdType, random_string, \
    trigger_async_del_size


class LuaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = RedisWrapper.get_instance()

        cls.k1 = '__lua1__'
        cls.k2 = '__lua2__'
        cls.k3 = '__lua3__'

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
        self.r.execute_command('del', self.k3)
        pass

    def run_script(self, script="", num_keys=0, *args):
        return self.r.execute_command('eval', script, num_keys, *args)

    def test_type_conversion(self):
        self.assertEqual(self.run_script("return 'hello'"), "hello")
        self.assertEqual(self.run_script("return 100.5"), 100)
        self.assertEqual(self.run_script("return true"), 1)
        self.assertEqual(self.run_script("return false"), None)
        self.assertEqual(self.run_script("return {ok='fine'}"), "fine")
        with self.assertRaises(Exception) as cm:
            self.run_script("return {err='ERR this is an error'}")
        err = cm.exception
        self.assertEqual(str(err), 'this is an error')
        self.assertListEqual(self.run_script("return {1,2,3,'ciao',{1,2}}"), [1, 2, 3, "ciao", [1, 2]])

    def test_args_populate(self):
        self.assertEqual(self.run_script("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", 2, "key1", "key2", "arg1", "arg2"), ["key1", "key2", "arg1", "arg2"])
    
    def test_eval_sha(self):
        sha = self.r.execute_command('script', 'load', 'return 1')
        self.assertEqual(self.r.execute_command('evalsha', sha, 0), 1)
        self.assertEqual(self.r.execute_command('evalsha', sha.upper(), 0), 1)
        with self.assertRaisesRegex(Exception, "No matching script"):
            self.r.execute_command('evalsha', 'not-exist-sha', 0)

    def test_integer_conversion(self):
        script = '''
        redis.call('set', KEYS[1], 0)
        local foo = redis.call('incr', KEYS[1])
        return {type(foo), foo}
        '''
        self.assertEqual(self.run_script(script, 1, self.k1), ['number', 1])

    def test_bulk_conversion(self):
        script = '''
        local res = redis.call('rpush', '__lua1__', 'a', 'b', 'c')
        local foo = redis.call('lrange', '__lua1__', 0, -1)
        return {type(foo), foo[1], foo[2], foo[3], # foo}
        '''
        #self.r.execute_command('rpush', self.k1, 'a', 'b', 'c')
        self.assertEqual(self.run_script(script, 1, self.k1), ['table', 'a', 'b', 'c', 3])

    def test_status_conversion(self):
        script = '''
        local foo = redis.call('set', KEYS[1], 'value')
        return {type(foo), foo['ok']}
        '''
        self.assertEqual(self.run_script(script, 1, self.k1), ['table', 'OK'])

    def test_error_conversion(self):
        script = '''
        redis.call('set', KEYS[1], 'a')
        local foo = redis.pcall('incr', KEYS[1])
        return {type(foo), foo['err']}
        '''
        self.assertEqual(self.run_script(script, 1, self.k1), ['table', 'ERR value is not an integer or out of range'])

    def test_nil_conversion(self):
        script = '''
        local foo = redis.call('get', KEYS[1])
        return {type(foo), foo == false}
        '''
        self.assertEqual(self.run_script(script, 1, self.k1), ['boolean', 1])

    def test_no_args(self):
        script = '''
        redis.call()
        '''
        with self.assertRaises(Exception) as cm:
            self.run_script(script)
        err = cm.exception
        self.assertTrue('one argument' in str(err))

    def test_unknown_cmds(self):
        script = '''
        redis.call('nosuchcommand')
        '''
        with self.assertRaises(Exception) as cm:
            self.run_script(script)
        err = cm.exception
        self.assertTrue('Invalid arguments' in str(err))

    
    def test_wrong_number_args(self):
        script = '''
        redis.call('get', 'a', 'b')
        '''
        with self.assertRaises(Exception) as cm:
            self.run_script(script)
        err = cm.exception
        self.assertTrue('Invalid arguments' in str(err))

    def test_error_reply(self):
        with self.assertRaises(Exception) as cm:
            self.run_script('return redis.error_reply("this is an error")')
        err = cm.exception
        self.assertTrue('this is an error' in str(err))

    def test_status_reply(self):
        self.assertEqual(self.run_script('return redis.status_reply("this is a status")'), 'this is a status')

    def test_sha1hex(self):
        script = 'return redis.sha1hex("foo")'
        self.assertEqual(self.run_script(script), '0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33')

    def test_type_error(self):
        script = '''
        redis.call('set', KEYS[1], 'b')
        return redis.call('sadd', KEYS[1], 'val')
        '''
        with self.assertRaises(Exception) as cm:
            self.run_script(script, 1, self.k1)
        err = cm.exception
        self.assertTrue('wrong kind of value' in str(err))

    def test_trailing_comments(self):
        script = '''
        return 1 --trailing comment
        '''
        self.assertEqual(self.run_script(script), 1)

    def test_many_args_command(self):
        script = '''
        local i
        local x={}
        for i=1,100 do
            table.insert(x,i)
        end
        redis.call('rpush', KEYS[1], unpack(x))
        return redis.call('lrange',KEYS[1],0,-1)
        '''
        self.assertListEqual(self.run_script(script, 1, self.k1), [str(i) for i in range(1,101)])

    # see redis issue https://github.com/redis/redis/issues/1118
    # see also mlua issue https://github.com/khvzak/mlua/issues/183
    def test_number_precision(self):
        script = '''
        local value = 9007199254740991;
        redis.call('set', KEYS[1], value);
        return redis.call('get', KEYS[1]);
        '''
        self.assertEqual(self.run_script(script, 1, self.k1), '9007199254740991')

    def test_str_number_precision(self):
        script = '''
        redis.call('set', KEYS[1], '12039611435714932082');
        return redis.call('get', KEYS[1]);
        '''
        self.assertEqual(self.run_script(script, 1, self.k1), '12039611435714932082')

    def test_negtive_argument_count(self):
        with self.assertRaises(Exception) as cm:
            self.run_script("return 'hello'", -12)
        err = cm.exception
        self.assertTrue('Invalid arguments' in str(err))

    # see issue https://github.com/redis/redis/issues/1939
    def test_reuse_argv(self):
        script = '''
        for i = 0, 10 do
            redis.call('set', KEYS[1], 'a')
            redis.call('mget', KEYS[1], KEYS[2], KEYS[3])
            redis.call('expire', KEYS[1], 0)
            redis.call('get', KEYS[1])
            redis.call('mget', KEYS[1], KEYS[2], KEYS[3])
        end
        return 0
        '''
        self.run_script(script, 3, self.k1, self.k2, self.k3)

    def test_nil_bulk_reply(self):
        script = '''
        redis.call('del', KEYS[1])
        local foo = redis.call('get', KEYS[1])
        return {type(foo), foo == false}
        '''
        self.assertListEqual(self.run_script(script, 1, self.k1), ['boolean', 1])

    def test_decr_if_gt(self):
        script = '''
        local current
        current = redis.call('get', KEYS[1])
        if not current then return nil end
        if current > ARGV[1] then
            return redis.call('decr', KEYS[1])
        else
            return redis.call('get', KEYS[1])
        end
        '''
        self.r.set(self.k1, 5)
        self.assertEqual(str(self.run_script(script, 1, self.k1, 2)), '4')
        self.assertEqual(str(self.run_script(script, 1, self.k1, 2)), '3')
        self.assertEqual(str(self.run_script(script, 1, self.k1, 2)), '2')
        self.assertEqual(str(self.run_script(script, 1, self.k1, 2)), '2')
        self.assertEqual(str(self.run_script(script, 1, self.k1, 2)), '2')

    def test_prng_seed(self):
        script = '''
        math.randomseed(ARGV[1]); return tostring(math.random())
        '''
        self.assertEqual(self.run_script(script, 0, 10), self.run_script(script, 0, 10))
        self.assertNotEqual(self.run_script(script, 0, 10), self.run_script(script, 0, 20))

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

    def test_string_async_del(self):
        v = random_string(trigger_async_del_size())
        self.assertTrue(self.execute_eval('set', self.k1, v))
        self.assertEqual(self.execute_eval('get', self.k1), v)
        self.assertTrue(self.execute_eval('del', self.k1))
        self.assertIsNone(self.execute_eval('get', self.k1))
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))

    def test_string_async_expire(self):
        v = random_string(trigger_async_del_size())
        self.assertTrue(self.execute_eval('set', self.k1, v))
        self.assertEqual(self.execute_eval('get', self.k1), v)
        self.assertTrue(self.execute_eval('expire', self.k1, 1))
        time.sleep(1)
        self.assertIsNone(self.execute_eval('get', self.k1))
        self.assertTrue(self.execute_eval('set', self.k1, self.v1))

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

    def test_hash_async_del(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.execute_eval('hset', self.k1, str(i), str(i)))
        for i in range(size):
            self.assertEqual(self.execute_eval('hget', self.k1, str(i)), str(i))
        self.assertTrue(self.execute_eval('del', self.k1))
        self.assertEqual(self.execute_eval('hlen', self.k1), 0)
        self.assertTrue(self.execute_eval('hset', self.k1, self.f1, self.v1))

    def test_hash_async_expire(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.execute_eval('hset', self.k1, str(i), str(i)))
        for i in range(size):
            self.assertEqual(self.execute_eval('hget', self.k1, str(i)), str(i))
        self.assertTrue(self.execute_eval('expire', self.k1, 1))
        time.sleep(1)
        self.assertEqual(self.execute_eval('hlen', self.k1), 0)
        self.assertTrue(self.execute_eval('hset', self.k1, self.f1, self.v1))

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
        self.assertEqual(self.execute_eval('linsert', self.k1, 'before', '0', 'hello1'), llen + 1)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1), ['hello1'] + [str(i) for i in range(0, 100)])
        # test insert after the first element
        self.assertEqual(self.execute_eval('linsert', self.k1, 'after', 'hello1', 'hello2'), llen + 2)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1),
                             ['hello1', 'hello2'] + [str(i) for i in range(0, 100)])
        # test insert in the middle
        self.assertEqual(self.execute_eval('linsert', self.k1, 'before', '50', 'hello3'), llen + 3)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1),
                             ['hello1', 'hello2'] + [str(i) for i in range(0, 50)] + ['hello3'] + [str(i) for i in
                                                                                                   range(50, 100)])
        self.assertEqual(self.execute_eval('linsert', self.k1, 'after', '50', 'hello4'), llen + 4)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1),
                             ['hello1', 'hello2'] + [str(i) for i in range(0, 50)] + ['hello3', '50', 'hello4'] + [
                                 str(i) for i in range(51, 100)])
        # test insert before the last element
        self.assertEqual(self.execute_eval('linsert', self.k1, 'before', '99', 'hello5'), llen + 5)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1),
                             ['hello1', 'hello2'] + [str(i) for i in range(0, 50)] + ['hello3', '50', 'hello4'] + [
                                 str(i) for i in range(51, 99)] + ['hello5', '99'])
        # test insert after the last element
        self.assertEqual(self.execute_eval('linsert', self.k1, 'after', '99', 'hello6'), llen + 6)
        self.assertListEqual(self.execute_eval('lrange', self.k1, 0, -1),
                             ['hello1', 'hello2'] + [str(i) for i in range(0, 50)] + ['hello3', '50', 'hello4'] + [
                                 str(i) for i in range(51, 99)] + ['hello5', '99', 'hello6'])

    def test_list_async_del(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        for i in range(size):
            self.assertEqual(self.execute_eval('lindex', self.k1, i), str(i))
        self.assertTrue(self.execute_eval('expire', self.k1, 1))
        time.sleep(1)
        self.assertEqual(self.execute_eval('llen', self.k1), 0)
        self.assertTrue(self.execute_eval('rpush', self.k1, self.v1))

    def test_list_async_expire(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.execute_eval('rpush', self.k1, str(i)))
        for i in range(size):
            self.assertEqual(self.execute_eval('lindex', self.k1, i), str(i))
        self.assertTrue(self.execute_eval('del', self.k1))
        self.assertEqual(self.execute_eval('llen', self.k1), 0)
        self.assertTrue(self.execute_eval('rpush', self.k1, self.v1))

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

    def test_set_async_del(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.execute_eval('sadd', self.k1, str(i)))
        for i in range(size):
            self.assertTrue(self.execute_eval('sismember', self.k1, str(i)))
        self.assertTrue(self.execute_eval('del', self.k1))
        self.assertEqual(self.execute_eval('scard', self.k1), 0)
        self.assertTrue(self.execute_eval('sadd', self.k1, self.v1))

    def test_set_async_expire(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.execute_eval('sadd', self.k1, str(i)))
        for i in range(size):
            self.assertTrue(self.execute_eval('sismember', self.k1, str(i)))
        self.assertTrue(self.execute_eval('expire', self.k1, 1))
        time.sleep(1)
        self.assertEqual(self.execute_eval('scard', self.k1), 0)
        self.assertTrue(self.execute_eval('sadd', self.k1, self.v1))

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

    def test_zset_async_del(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.execute_eval('zadd', self.k1, i, str(i)))
        for i in range(size):
            self.assertEqual(int(self.execute_eval('zscore', self.k1, str(i))), i)
        self.assertTrue(self.execute_eval('del', self.k1))
        self.assertEqual(self.execute_eval('zcard', self.k1), 0)
        self.assertTrue(self.execute_eval('zadd', self.k1, 1, self.v1))

    def test_zset_async_expire(self):
        size = trigger_async_del_size()
        for i in range(size):
            self.assertTrue(self.execute_eval('zadd', self.k1, i, str(i)))
        for i in range(size):
            self.assertEqual(int(self.execute_eval('zscore', self.k1, str(i))), i)
        self.assertTrue(self.execute_eval('expire', self.k1, 1))
        time.sleep(1)
        self.assertEqual(self.execute_eval('zcard', self.k1), 0)
        self.assertTrue(self.execute_eval('zadd', self.k1, 1, self.v1))

    # ================ generic ================
    def test_type(self):
        for cmd_type in CmdType:
            self.assertEqual(self.execute_eval('type', self.k1), CmdType.NULL.value)

            if cmd_type is CmdType.STRING:
                self.assertTrue(self.execute_eval('set', self.k1, self.v1))
                self.assertEqual(self.execute_eval('type', self.k1), CmdType.STRING.value)
            elif cmd_type is CmdType.HASH:
                self.assertEqual(self.execute_eval('hset', self.k1, self.f1, self.v1), 1)
                self.assertEqual(self.execute_eval('type', self.k1), CmdType.HASH.value)
            elif cmd_type is CmdType.LIST:
                self.assertTrue(self.execute_eval('lpush', self.k1, self.v1))
                self.assertEqual(self.execute_eval('type', self.k1), CmdType.LIST.value)
            elif cmd_type is CmdType.SET:
                self.assertEqual(self.execute_eval('sadd', self.k1, self.v1), 1)
                self.assertEqual(self.execute_eval('type', self.k1), CmdType.SET.value)
            elif cmd_type is CmdType.ZSET:
                self.assertEqual(self.execute_eval('zadd', self.k1, 1, self.v1), 1)
                self.assertEqual(self.execute_eval('type', self.k1), CmdType.ZSET.value)
            elif cmd_type is CmdType.NULL:
                self.assertEqual(self.execute_eval('type', self.k2), CmdType.NULL.value)
            else:
                raise Exception("there is an uncovered command type: " + cmd_type.name)

            if cmd_type is not CmdType.NULL:
                self.assertTrue(self.execute_eval('del', self.k1))

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
        cls.r.execute_command('script', 'flush')
        cls.r.execute_command('del', cls.k1)
        cls.r.execute_command('del', cls.k2)
        print('test scripts flush')
