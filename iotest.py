from libiotest.sqlite import SQLite3Test
from libiotest.mongo import MongoTest
from libiotest.lib import parse_size, print_result
import argparse

parser = argparse.ArgumentParser('iotest')
parser.add_argument('--type', '-t', required=True, type=str, choices=['mongo', 'sqlite'])
parser.add_argument('--dir', '-d', required=True, type=str)
parser.add_argument('--times', '-n', type=int, default=1)
parser.add_argument('--port', '-p', type=int, default=8899)
parser.add_argument('--size', '-s', required=True, type=str)
parser.add_argument('--count', '-c', required=True, type=str)
parser.add_argument('--batch', '-b', required=True, type=int)
parser.add_argument('--host', '-i', type=str, default='localhost')
args = parser.parse_args()

if args.type == 'mongo':
    test = MongoTest(args.dir, args.host, args.port)
elif args.type == 'sqlite':
    test = SQLite3Test(args.dir)
else:
    print('unsupported type')
    exit(1)

size = parse_size(args.size)
count = parse_size(args.count)

result = test.run(size, count, args.batch, args.times)

print_result(result, args.dir, args.type, size, count, args.batch, args.times)
