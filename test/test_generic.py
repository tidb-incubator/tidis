import unittest

from redis import exceptions

from rediswrap import RedisWrapper
from test_util import random_string


class GenericTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = RedisWrapper.get_instance()
        cls.k1 = '__key1__'
        cls.k2 = '__key2__'

    def setUp(self):
        self.r.execute_command('del', self.k1)
        self.r.execute_command('del', self.k2)
        pass

    @unittest.skipUnless(RedisWrapper.requirepass, "skip auth when requirepass is false")
    def test_auth(self):
        self.assertRaises(exceptions.AuthenticationError, self.r.ping)
        self.assertTrue(RedisWrapper.auth())
        self.assertTrue(self.r.ping())

    def test_ping(self):
        self.assertTrue(self.r.ping())

    def test_multi_exec(self):
        self.assertTrue(self.r.execute_command('multi'))
        self.r.execute_command('set', self.k1, 'value1')
        self.assertEqual(self.r.get(self.k1), 'QUEUED')
        self.assertListEqual(self.r.execute_command('exec'), ['OK', 'value1'])

    def test_multi_discard(self):
        self.assertTrue(self.r.execute_command('multi'))
        self.r.execute_command('set', self.k1, 'value1')
        self.assertEqual(self.r.execute_command('discard'), 'OK')

    def test_multi_empty(self):
        self.assertTrue(self.r.execute_command('multi'))
        self.assertListEqual(self.r.execute_command('exec'), [])

    def test_multi_error(self):
        self.assertTrue(self.r.execute_command('multi'))
        with self.assertRaises(Exception) as cm:
            self.r.execute_command('multi')
        err = cm.exception
        self.assertEqual(str(err), 'MULTI calls can not be nested')
        self.assertEqual(self.r.execute_command('discard'), 'OK')
        with self.assertRaises(Exception) as cm:
            self.r.execute_command('exec')
        err = cm.exception
        self.assertEqual(str(err), 'EXEC without MULTI')
        with self.assertRaises(Exception) as cm:
            self.r.execute_command('discard')
        err = cm.exception
        self.assertEqual(str(err), 'DISCARD without MULTI')

    def test_client(self):
        client1 = self.r
        client1_id = client1.execute_command("client id")
        self.assertIsNotNone(client1_id)

        self.assertIsNone(client1.execute_command("client getname"))
        random_name = random_string(6)
        self.assertTrue(client1.execute_command("client setname", random_name))
        self.assertEqual(client1.execute_command("client getname"), random_name)

        client2 = RedisWrapper.clone()
        self.assertIsNotNone(client2.execute_command("client id"))
        self.assertIsNotNone(client2.execute_command("client list id", client1_id))
        self.assertEqual(client2.execute_command("client kill id", client1_id), 1)
        self.assertEqual(client2.execute_command("client list id", client1_id), "")

    def test_scan(self):
        # add some keys for scan test
        for i in range(0, 10):
            self.r.set('string:' + str(i), 'value' + str(i))
            self.r.lpush('list:' + str(i), 'value' + str(i))
            self.r.sadd('set:' + str(i), 'value' + str(i))
            self.r.hset('hash:' + str(i), 'key' + str(i), 'value' + str(i))
            self.r.zadd('zset:' + str(i), {'value' + str(i): i})
        # use the custom xscan command to test the scan implementation
        # to avoid the cursor converting to int
        # xscan is the same as scan in server side
        all_scan = self.r.execute_command('xscan', '', 'count', 100)
        self.assertEqual(all_scan[0], '')
        self.assertEqual(len(all_scan[1]), 50)
        part1_scan = self.r.execute_command('xscan', '', 'count', 10)
        self.assertEqual(part1_scan[0], 'hash:9')
        self.assertEqual(len(part1_scan[1]), 10)
        part2_scan = self.r.execute_command('xscan', part1_scan[0], 'count', 10)
        self.assertEqual(part2_scan[0], 'list:9')
        self.assertEqual(len(part2_scan[1]), 10)
        match_scan = self.r.execute_command('xscan', '', 'count', 100, 'match', '^hash:*')
        self.assertEqual(match_scan[0], '')
        self.assertEqual(len(match_scan[1]), 10)

        # clean up the keys
        keys = []
        for i in range(0, 10):
            keys.append('string:' + str(i))
            keys.append('list:' + str(i))
            keys.append('set:' + str(i))
            keys.append('hash:' + str(i))
            keys.append('zset:' + str(i))
        self.r.delete(*keys)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('del', cls.k1)
        cls.r.execute_command('del', cls.k2)
        print('test data cleaned up')
