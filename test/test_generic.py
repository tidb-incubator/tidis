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

    def test_scan_invalid_arguments(self):
        self.r.set(self.k1, 'unchanged')
        invalid = [
            (), ('', 'COUNT'), ('', 'MATCH'), ('', 'UNKNOWN'),
            ('', 'UNKNOWN', 'value'), ('', 'COUNT', '-1'),
            ('', 'COUNT', '0'), ('', 'COUNT', '4294967296'),
            ('', 'COUNT', '9223372036854775808'), ('', 'COUNT', ''),
            ('', 'COUNT', 'abc'), ('', 'COUNT', '1.5'),
            ('', b'\xff', 'value'),
            ('', 'COUNT', '1', 'MATCH'), ('', 'MATCH', '.*', 'COUNT'),
            ('', 'MATCH', '.*', 'UNKNOWN', 'value'),
            ('', 'COUNT', '-1', 'COUNT', '1'),
            ('', 'MATCH', '['), ('', 'COUNT', '1', 'MATCH', '['),
        ]
        conn = self.r.connection_pool.get_connection('SCAN')
        try:
            conn.connect()
            sock = conn._sock
            for command in ('SCAN', 'XSCAN'):
                for mode in ('tcp', 'call', 'pcall'):
                    for args in invalid:
                        with self.subTest(command=command, mode=mode, args=args):
                            if mode == 'tcp':
                                conn.send_command(command, *args)
                            else:
                                script = 'return redis.{}(unpack(ARGV))'.format(mode)
                                conn.send_command('EVAL', script, 0, command, *args)
                            with self.assertRaises(exceptions.ResponseError) as error:
                                conn.read_response()
                            expected = ('invalid regular expression' if args and args[-1] == '['
                                        else 'Invalid arguments')
                            self.assertEqual(str(error.exception), expected)
                            self.assertIs(conn._sock, sock)
                            conn.send_command('PING')
                            self.assertEqual(conn.read_response(), 'PONG')
                            self.assertIs(conn._sock, sock)
                            conn.send_command('GET', self.k1)
                            self.assertEqual(conn.read_response(), 'unchanged')
        finally:
            self.r.connection_pool.release(conn)

    def test_scan_option_boundaries(self):
        self.r.set(self.k1, 'unchanged')
        options = [
            (), ('cOuNt', '1'), ('COUNT', '4294967295'),
            ('MATCH', '^__key1__$', 'COUNT', '10'),
            ('COUNT', '10', 'MATCH', '^__key1__$'),
        ]
        # SCAN's cursor is a key string; bypass redis-py's integer conversion.
        conn = self.r.connection_pool.get_connection('SCAN')
        try:
            for command in ('SCAN', 'XSCAN'):
                for args in options:
                    expected = [self.k1 if args == ('cOuNt', '1') else '', [self.k1]]
                    for mode in ('tcp', 'call', 'pcall'):
                        with self.subTest(command=command, mode=mode, args=args):
                            if mode == 'tcp':
                                conn.send_command(command, '', *args)
                            else:
                                script = 'return redis.{}(unpack(ARGV))'.format(mode)
                                conn.send_command('EVAL', script, 0, command, '', *args)
                            self.assertEqual(conn.read_response(), expected)
        finally:
            self.r.connection_pool.release(conn)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('del', cls.k1)
        cls.r.execute_command('del', cls.k2)
        print('test data cleaned up')
