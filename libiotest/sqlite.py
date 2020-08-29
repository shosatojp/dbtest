from sqlite3.dbapi2 import Connection, Cursor
from .lib import DBTest, drop_caches
import sqlite3
import os
from tqdm import tqdm
import subprocess
import math
import random
import time


class SQLite3Test(DBTest):
    def __init__(self, dir) -> None:
        self.dir = dir
        return

    def _run1(self, fn, what=''):
        drop_caches()
        conn = sqlite3.connect(self.path)
        cur = conn.cursor()

        t = self._stopwatch(fn, conn, cur)
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
        self.path = os.path.join(self.dir, f'sqlite3-test-{time.time()}-{self.size}-{count}-{batch}.db')
        print('creating dummy data...')
        self.dummy = [os.urandom(self.size) for _ in range(self.count)]

        ret = []
        for _ in range(times):
            conn = sqlite3.connect(self.path)
            cur = conn.cursor()
            cur.execute('create table test(id integer,data blob)')
            cur.execute('create index idindex on test(id)')
            cur.close()
            conn.close()

            result = {}
            result.update(self._run1(self._seq_write, what='seq_write'))
            result.update(self._run1(self._seq_read, what='seq_read'))
            result.update(self._run1(self._rand_read, what='rand_read'))
            result.update(self._run1(self._rand_write, what='rand_write'))
            os.remove(self.path)
            ret.append(result)
        return ret

    def _seq_write(self, conn: Connection, cur: Cursor):
        print('sqlite write')
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    buffer.append((j, self.dummy[j]))
                cur.executemany('insert into test values (?,?)', buffer)
                conn.commit()

    def _seq_read(self, conn: Connection, cur: Cursor):
        print('sqlite seq read')
        with tqdm(total=self.count) as bar:
            cur.execute('select * from test')
            for _ in cur:
                bar.update(1)

    def _rand_read(self, conn: Connection, cur: Cursor):
        print('sqlite rand read')
        with tqdm(total=self.count) as bar:
            for _ in range(0, self.count, self.batch):
                cur.execute(f'select * from test where id in ({",".join(["?"]*self.batch)})',
                            [math.floor(random.random()*self.count) for _ in range(self.batch)])
                cur.fetchall()
                bar.update(self.batch)

    def _rand_write(self, conn: Connection, cur: Cursor):
        print('sqlite rand write')
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    id = math.floor(random.random()*self.count)
                    buffer.append((id,
                                   self.dummy[id]))
                cur.executemany('update test set data = ? where id = ?', buffer)
                conn.commit()
