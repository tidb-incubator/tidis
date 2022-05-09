import redis


class RedisWrapper:
    default_ip = "172.16.5.32"
    default_port = 6379

    def __init__(self, ip=default_ip, port=default_port):
        print("connecting to {}:{}...".format(ip, port))
        self.r = redis.StrictRedis(host=ip, port=port, decode_responses=True)
        self.test_connection()

    def test_connection(self):
        if self.r.ping():
            print("connection succeeded")
        else:
            print("connection failed")

    def get_instance(self):
        return self.r
