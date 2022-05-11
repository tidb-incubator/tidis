import unittest

from redis import exceptions

from rediswrap import RedisWrapper


class GenericTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = RedisWrapper.get_instance()

    @unittest.skipUnless(RedisWrapper.requirepass, "skip auth when requirepass is false")
    def test_auth(self):
        self.assertRaises(exceptions.AuthenticationError, self.r.ping)
        self.assertTrue(RedisWrapper.auth())
        self.assertTrue(self.r.ping())

    def test_ping(self):
        self.assertTrue(self.r.ping())
