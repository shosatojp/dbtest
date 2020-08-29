from pprint import pprint
from time import sleep

from pymongo.collection import Collection
from .lib import DBTest
import os
from tqdm import tqdm
import subprocess
import math
import random
from pymongo import ASCENDING, MongoClient, UpdateOne
import shutil
import time


class MongoTest(DBTest):
    def __init__(self, dir, host, port) -> None:
        self.dir = os.path.join(dir, 'mongodb-test-'+str(int(time.time())))
        if os.path.exists(self.dir):
            ans = input(f'remove dir : {self.dir} ? [y/N]')
            if ans.lower() == 'y':
                shutil.rmtree(self.dir)
            else:
                exit(1)
        os.mkdir(self.dir)
        self.host = host
        self.port = port
        self.dummy = None
        return

    def _run1(self, fn, what=''):
        subprocess.call(['bash', './drop_caches.sh'])
        p = subprocess.Popen(['mongod', '--dbpath', self.dir, '--bind_ip', self.host, '--port', str(self.port), '--quiet'])
        sleep(3)
        conn = MongoClient(self.host, self.port)
        cur = conn['db']['collection']
        cur.create_index([('id', ASCENDING)], unique=True)

        t = self._stopwatch(fn, cur)
        result = {
            what: {
                'name': 'mongodb',
                'what': what,
                'size': self.size,
                'count': self.count,
                'batch': self.batch,
                'time': t,
                'speed': self.count/(t+1e-3),
            }
        }
        print(result)
        conn.close()
        p.terminate()
        sleep(3)
        return result

    def run(self, size, count, batch, times=1):
        self.size = size
        self.count = count
        self.batch = batch

        print('creating dummy data...')
        self.dummy = [os.urandom(self.size) for _ in range(self.count)]

        ret = []
        for _ in range(times):
            os.mkdir(self.dir)
            result = {}
            result.update(self._run1(self._seq_write, what='seq_write'))
            result.update(self._run1(self._rand_read, what='rand_read'))
            result.update(self._run1(self._seq_read, what='seq_read'))
            result.update(self._run1(self._rand_write, what='rand_write'))
            shutil.rmtree(self.dir)
            ret.append(result)
        return ret

    def _seq_write(self, col: Collection):
        print('mongodb write')
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    buffer.append({'id': j, 'data': self.dummy[j]})
                col.insert_many(buffer)

    def _seq_read(self, col: Collection):
        print('mongodb seq read')
        with tqdm(total=self.count) as bar:
            cur = col.find({})
            cur.batch_size(self.batch)
            for _ in cur:
                bar.update(1)

    def _rand_read(self, col: Collection):
        print('mongodb rand read')
        with tqdm(total=self.count) as bar:
            for _ in range(0, self.count, self.batch):
                cur = col.find({
                    'id': {'$in':  [math.floor(random.random()*self.count) for _ in range(self.batch)]}
                })
                cur.batch_size(self.batch)
                for _ in cur:
                    bar.update(1)

    def _rand_write(self, col: Collection):
        print('mongodb rand write')
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    id = math.floor(random.random()*self.count)
                    buffer.append(
                        UpdateOne(
                            {'id': id},
                            {'$set': {'data': self.dummy[id]}},
                        )
                    )
                col.bulk_write(buffer)


if __name__ == "__main__":
    t = MongoTest(
        '/home/sho/dbtest',
        host='localhost',
        port=8889,
        size=100*1024,
        count=10000,
        batch=100
    )
    pprint(t.run())
