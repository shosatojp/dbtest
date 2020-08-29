import math
import itertools
import subprocess
from typing import Tuple
import time
import re


class DBTest():
    def _stopwatch(self, fn, *args, msg=''):
        s = time.time()
        fn(*args)
        f = time.time()
        return f-s

    def run(self, size, count, batch, times=1):
        pass


def parse_size(src: str):
    m = re.match('^(\d+)([kmgt]?)(i?)b?$', src.lower())
    if m:
        if m[2]:
            return int(int(m[1]) * math.pow(1000+(m[3] == 'i')*24, 1+'kmgt'.index(m[2])))
        else:
            return int(m[1])
    else:
        print('invalid size')
        exit(0)


def print_result(result, type, size, count, batch, times):
    print()
    print(f'{type}: (size={size}, count={count}, batch={batch}, times={times})')
    for what in ['seq_write', 'seq_read', 'rand_write', 'rand_read']:
        sum_time = sum(map(lambda e: e[what]['time'], result))
        print(f'{what:10} {int(count/(sum_time/times+1e-3)):>7}/s')


def drop_caches():
    subprocess.call(['bash', './drop_caches.sh'])
