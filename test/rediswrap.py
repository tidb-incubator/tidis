import argparse

import redis


class RedisWrapper:
    _instance = None

    default_ip = "127.0.0.1"
    default_port = 6379

    requirepass = False
    # auth password when requirepass is true
    password = ""

    @classmethod
    def set_instance_manually(cls, ip=default_ip, port=default_port):
        cls._set_instance(cls, ip, port)

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            ip, port = cls.parse_arguments(cls)
            cls._set_instance(cls, ip, port)
        return cls._instance

    @classmethod
    def auth(cls):
        r = cls.get_instance()
        return r.execute_command('auth', cls.password)

    def _set_instance(self, ip, port):
        print("connecting to {}:{}...".format(ip, port))
        self._instance = redis.StrictRedis(host=ip, port=port, decode_responses=True)
        print("connection completed")

    def parse_arguments(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-i', '--ip', default=self.default_ip,
                            help="service host (default: {})".format(self.default_ip))
        parser.add_argument('-p', '--port', default=self.default_port,
                            help="service port (default: {})".format(self.default_port))
        args = parser.parse_args()
        return args.ip, args.port
