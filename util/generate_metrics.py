#!/usr/bin/env python

import random
import string
import time
import timeit

from datetime import timedelta, datetime
from sys import stderr


METRICS = ["one", "two", "three"] # , "four", "five", "six"]

def random_string(N):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(N))
   
if __name__ == '__main__':
    now = datetime.now() + timedelta(hours = 4)
    past = now - timedelta(hours=12)

    start = time.mktime(past.timetuple())
    stop  = time.mktime(now.timetuple())
    last_checkpoint = timeit.time.time()

    updates = 0
    while start <= stop:
        num_metrics = 3 # random.randint(2, 7)
        metric = '.'.join(random.choice(METRICS) for x in range(num_metrics))
        value = random.random() * 1000
        timestamp = int(time.mktime(datetime.now().timetuple())) #random.randint(start, stop)
        print '%s %s %s' % (metric, value, timestamp)
        updates += 1

        if updates % 5000 == 0:
            checkpoint = timeit.time.time()
            delta = (checkpoint - last_checkpoint)
            stderr.write("%.2f req/s   \r" % (updates / delta))
            last_checkpoint = checkpoint
            updates = 0
