from sqlite3.dbapi2 import Connection, Cursor
from lib import DBTest
import sqlite3
import os
from tqdm import tqdm
import subprocess
import math
import random


class SQLite3Test(DBTest):
    def __init__(self, dir, size, count, batch) -> None:
        self.dir = dir
        self.size = size
        self.count = count
        self.batch = batch
        self.path = os.path.join(self.dir, f'sqlite3-test-{self.size}-{count}-{batch}.db')
        return

    def _run1(self, fn, what=''):
        subprocess.call(['bash', './drop_caches.sh'])
        conn = sqlite3.connect(self.path)
        cur = conn.cursor()

        t = self._stopwatch(fn, conn, cur)
        result = f'sqlite3 {what:10}: size={self.size}, count={self.count}, batch={self.batch} : {int(t*1000)/1000}s, {int(self.count/(t+1e-3))}/s\n'

        cur.close()
        conn.close()
        return result

    def run(self):
        conn = sqlite3.connect(self.path)
        cur = conn.cursor()
        cur.execute('create table if not exists test(id integer,data blob)')
        cur.close()
        conn.close()

        result = ''
        result += self._run1(self._seq_write, what='seq_write')
        result += self._run1(self._seq_read, what='seq_read')
        result += self._run1(self._rand_read, what='rand_read')
        result += self._run1(self._rand_write, what='rand_write')
        os.remove(self.path)
        return result

    def _seq_write(self, conn: Connection, cur: Cursor):
        print('sqlite write')
        with tqdm(total=self.count) as bar:
            for i in range(0, self.count, self.batch):
                bar.update(n=self.batch)
                buffer = []
                for j in range(i, min(i+self.batch, self.count)):
                    buffer.append((j, os.urandom(self.size)))
                cur.executemany('insert into test values (?,?)', buffer)
                conn.commit()

    def _seq_read(self, conn: Connection, cur: Cursor):
        print('sqlite seq read')
        with tqdm(total=self.count) as bar:
            cur.execute('select * from test')
            for _ in range(self.count):
                cur.fetchone()
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
                    buffer.append((math.floor(random.random()*self.count),
                                   os.urandom(self.size)))
                cur.executemany('update test set data = ? where id = ?', buffer)
                conn.commit()


if __name__ == "__main__":
    t = SQLite3Test('/mnt/data/datasets/test', size=150*1024, count=1000, batch=100)
    print(t.run())
