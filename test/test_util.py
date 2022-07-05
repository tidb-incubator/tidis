import random
import string
import time
from enum import Enum
from math import floor

NaN = float('nan')
NOT_EXISTS_LITERAL = "__not_exists__"


class CmdType(Enum):
    STRING = "string"
    HASH = "hash"
    LIST = "list"
    SET = "set"
    ZSET = "zset"
    NULL = "none"


def current_sec_ts():
    return int(floor(time.time()))


def current_msec_ts():
    return int(floor(time.time() * 1000))


def sec_ts_after_five_secs():
    return current_sec_ts() + 5


def msec_ts_after_five_secs():
    return current_msec_ts() + 5000


def random_string(n):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))
