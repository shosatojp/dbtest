from pprint import pprint
from time import sleep

from pymongo.collection import Collection
from .lib import DBTest, drop_caches
import os
from tqdm import tqdm
import subprocess
import math
import random
from pymongo import ASCENDING, MongoClient, UpdateOne
import shutil
import time
import matplotlib.pyplot as plt


class MongoTest(DBTest):
    def __init__(self, dir, host, port) -> None:
        self.dir = dir
        self.host = host
        self.port = port
        self.dummy = None
        return

    def _run1(self, fn, what=''):
        drop_caches()
        p = subprocess.Popen(['mongod', '--dbpath', self.path, '--bind_ip', self.host, '--port', str(self.port), '--quiet'])
        sleep(3)
        conn = MongoClient(self.host, self.port)
        cur = conn['db']['collection']
        cur.create_index([('id', ASCENDING)], unique=True)

        t, ret = self._stopwatch(fn, cur)
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
        for i in range(times):
            self.path = os.path.join(self.dir, 'mongodb-test-'+str(int(time.time())))
            os.mkdir(self.path)
            self.fig = plt.figure(figsize=(12, 8), dpi=300)
            result = {}
            result.update(self._run1(self._seq_write, what='seq_write'))
            result.update(self._run1(self._rand_read, what='rand_read'))
            result.update(self._run1(self._seq_read, what='seq_read'))
            result.update(self._run1(self._rand_write, what='rand_write'))
            self.fig.suptitle(f'mongo (size={size},count={count},batch={batch}) #{i+1}')
            self.fig.savefig(f'mongo-{int(time.time())}-{self.size}-{count}-{batch}-{i+1}.png')
            shutil.rmtree(self.path)
            ret.append(result)
        return ret

    def _seq_write(self, col: Collection):
        print('mongodb write')
        x, y, start = [], [], time.time()
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    buffer.append({'id': j, 'data': self.dummy[j]})
                col.insert_many(buffer)
                x.append(time.time()-start)
                y.append(min(i+self.batch, self.count))

        ax = self.fig.add_subplot(2, 2, 2)
        ax.plot(x, y)
        ax.set_title('seq write')

    def _seq_read(self, col: Collection):
        print('mongodb seq read')
        x, y, start = [], [], time.time()
        with tqdm(total=self.count) as bar:
            cur = col.find({})
            cur.batch_size(self.batch)
            for i, _ in enumerate(cur):
                bar.update(1)
                x.append(time.time()-start)
                y.append(i)

        ax = self.fig.add_subplot(2, 2, 1)
        ax.plot(x, y)
        ax.set_title('seq read')

    def _rand_read(self, col: Collection):
        print('mongodb rand read')
        x, y, start = [], [], time.time()
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                cur = col.find({
                    'id': {'$in':  [math.floor(random.random()*self.count) for _ in range(self.batch)]}
                })
                cur.batch_size(self.batch)
                for _, _ in enumerate(cur):
                    bar.update(1)
                x.append(time.time()-start)
                y.append(min(i+self.batch, self.count))

        ax = self.fig.add_subplot(2, 2, 3)
        ax.plot(x, y)
        ax.set_title('rand read')

    def _rand_write(self, col: Collection):
        print('mongodb rand write')
        x, y, start = [], [], time.time()
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
                x.append(time.time()-start)
                y.append(min(i+self.batch, self.count))

        ax = self.fig.add_subplot(2, 2, 4)
        ax.plot(x, y)
        ax.set_title('rand write')
