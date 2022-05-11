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
        cls._set_instance(ip, port)

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            ip, port = cls.parse_arguments()
            cls._set_instance(ip, port)
        return cls._instance

    @classmethod
    def auth(cls):
        r = cls.get_instance()
        return r.execute_command('auth', cls.password)

    @classmethod
    def _set_instance(cls, ip, port):
        print("connecting to {}:{}...".format(ip, port))
        cls._instance = redis.StrictRedis(host=ip, port=port, decode_responses=True)
        print("connection completed")

    @classmethod
    def parse_arguments(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument('-i', '--ip', default=cls.default_ip,
                            help="service host (default: {})".format(cls.default_ip))
        parser.add_argument('-p', '--port', default=cls.default_port,
                            help="service port (default: {})".format(cls.default_port))
        args = parser.parse_args()
        return args.ip, args.port
