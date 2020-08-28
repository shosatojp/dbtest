import time

class DBTest():
    def _stopwatch(self, fn, *args, msg=''):
        s = time.time()
        fn(*args)
        f = time.time()
        return f-s