import unittest

from redis import exceptions

from rediswrap import RedisWrapper


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

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.r.execute_command('del', cls.k1)
        cls.r.execute_command('del', cls.k2)
        print('test data cleaned up')
