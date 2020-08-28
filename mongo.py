from time import sleep

from pymongo.collection import Collection
from lib import DBTest
import os
from tqdm import tqdm
import subprocess
import math
import random
from pymongo import ASCENDING, MongoClient, UpdateOne
import shutil


class MongoTest(DBTest):
    def __init__(self, dir, host, port, size, count, batch) -> None:
        self.dir = dir
        self.size = size
        self.count = count
        self.batch = batch
        self.host = host
        self.port = port
        return

    def _run1(self, fn, what=''):
        subprocess.call(['bash', './drop_caches.sh'])
        p = subprocess.Popen(['mongod', '--dbpath', self.dir, '--bind_ip', self.host, '--port', str(self.port), '--quiet'])
        sleep(3)
        conn = MongoClient(self.host, self.port)
        cur = conn['db']['collection']
        cur.create_index([('id', ASCENDING)], unique=True)

        t = self._stopwatch(fn, cur)
        result = f'sqlite3 {what:10}: size={self.size}, count={self.count}, batch={self.batch} : {int(t*1000)/1000}s, {int(self.count/(t+1e-3))}/s\n'

        conn.close()
        p.terminate()
        sleep(3)
        return result

    def run(self):
        if os.path.exists(self.dir):
            ans = input(f'remove dir : {self.dir} ? [y/N]')
            if ans.lower() == 'y':
                shutil.rmtree(self.dir)
            else:
                exit(1)
        os.mkdir(self.dir)
        result = '\n'
        result += self._run1(self._seq_write, what='seq_write')
        result += self._run1(self._rand_read, what='rand_read')
        result += self._run1(self._seq_read, what='seq_read')
        result += self._run1(self._rand_write, what='rand_write')
        shutil.rmtree(self.dir)
        return result

    def _seq_write(self, col: Collection):
        print('sqlite write')
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    buffer.append({'id': j, 'data': os.urandom(self.size)})
                col.insert_many(buffer)

    def _seq_read(self, col: Collection):
        print('sqlite seq read')
        with tqdm(total=self.count) as bar:
            cur = col.find({})
            cur.batch_size(self.batch)
            for _ in cur:
                bar.update(1)

    def _rand_read(self, col: Collection):
        print('sqlite rand read')
        with tqdm(total=self.count) as bar:
            for _ in range(0, self.count, self.batch):
                cur = col.find({
                    'id': {'$in':  [math.floor(random.random()*self.count) for _ in range(self.batch)]}
                })
                cur.batch_size(self.batch)
                for _ in cur:
                    bar.update(1)

    def _rand_write(self, col: Collection):
        print('sqlite rand write')
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    buffer.append(
                        UpdateOne(
                            {'id': math.floor(random.random()*self.count)},
                            {'$set': {'data': os.urandom(self.size)}},
                        )
                    )
                col.bulk_write(buffer)


if __name__ == "__main__":
    t = MongoTest(
        '/mnt/data/datasets/test/db',
        host='localhost',
        port=8888,
        size=150*1024,
        count=10000,
        batch=100
    )
    print(t.run())
