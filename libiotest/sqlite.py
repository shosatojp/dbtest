from sqlite3.dbapi2 import Connection, Cursor
from .lib import DBTest, drop_caches
import sqlite3
import os
from tqdm import tqdm
import subprocess
import math
import random
import time
import matplotlib.pyplot as plt
import matplotlib.figure


class SQLite3Test(DBTest):
    def __init__(self, dir) -> None:
        self.dir = dir
        return

    def _run1(self, fn, what=''):
        drop_caches()
        conn = sqlite3.connect(self.path)
        cur = conn.cursor()

        t, ret = self._stopwatch(fn, conn, cur)
        result = {
            what: {
                'name': 'sqlite3',
                'what': what,
                'size': self.size,
                'count': self.count,
                'batch': self.batch,
                'time': t,
                'speed': self.count/(t+1e-3),
            }
        }
        print(result)
        cur.close()
        conn.close()
        return result

    def run(self, size, count, batch, times=1):
        self.size = size
        self.count = count
        self.batch = batch
        self.path = os.path.join(self.dir, f'sqlite3-test-{int(time.time())}-{self.size}-{count}-{batch}.db')
        print('creating dummy data...')
        self.dummy = [os.urandom(self.size) for _ in range(self.count)]

        ret = []
        for i in range(times):
            conn = sqlite3.connect(self.path)
            cur = conn.cursor()
            cur.execute('create table test(id integer,data blob)')
            cur.execute('create index idindex on test(id)')
            cur.close()
            conn.close()

            self.fig = plt.figure(figsize=(12, 8), dpi=300)
            result = {}
            result.update(self._run1(self._seq_write, what='seq_write'))
            result.update(self._run1(self._seq_read, what='seq_read'))
            result.update(self._run1(self._rand_read, what='rand_read'))
            result.update(self._run1(self._rand_write, what='rand_write'))
            self.fig.suptitle(f'sqlite (size={size},count={count},batch={batch}) #{i+1}')
            self.fig.savefig(f'sqlite-{int(time.time())}-{self.size}-{count}-{batch}-{i+1}.png')
            os.remove(self.path)
            ret.append(result)
        return ret

    def _seq_write(self, conn: Connection, cur: Cursor):
        print('sqlite write')
        x, y, start = [], [], time.time()
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    buffer.append((j, self.dummy[j]))
                cur.executemany('insert into test values (?,?)', buffer)
                conn.commit()
                x.append(time.time()-start)
                y.append(min(i+self.batch, self.count))

        ax = self.fig.add_subplot(2, 2, 2)
        ax.plot(x, y)
        ax.set_title('seq write')

    def _seq_read(self, conn: Connection, cur: Cursor):
        print('sqlite seq read')
        x, y, start = [], [], time.time()
        with tqdm(total=self.count) as bar:
            cur.execute('select * from test')
            for i, _ in enumerate(cur):
                bar.update(1)
                x.append(time.time()-start)
                y.append(i)

        ax = self.fig.add_subplot(2, 2, 1)
        ax.plot(x, y)
        ax.set_title('seq read')

    def _rand_read(self, conn: Connection, cur: Cursor):
        print('sqlite rand read')
        x, y, start = [], [], time.time()
        start = time.time()
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                cur.execute(f'select * from test where id in ({",".join(["?"]*self.batch)})',
                            [math.floor(random.random()*self.count) for _ in range(self.batch)])
                cur.fetchall()
                bar.update(self.batch)
                x.append(time.time()-start)
                y.append(min(i+self.batch, self.count))

        ax = self.fig.add_subplot(2, 2, 3)
        ax.plot(x, y)
        ax.set_title('rand read')

    def _rand_write(self, conn: Connection, cur: Cursor):
        print('sqlite rand write')
        x, y, start = [], [], time.time()
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    id = math.floor(random.random()*self.count)
                    buffer.append((self.dummy[id],
                                   id))
                cur.executemany('update test set data = ? where id = ?', buffer)
                conn.commit()
                x.append(time.time()-start)
                y.append(min(i+self.batch, self.count))

        ax = self.fig.add_subplot(2, 2, 4)
        ax.plot(x, y)
        ax.set_title('rand write')
