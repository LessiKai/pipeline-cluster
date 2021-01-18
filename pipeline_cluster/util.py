import multiprocessing as mp
import multiprocessing.connection as mpc
import time
from ctypes import c_int64

def connect_timeout(addr, retry=False, retry_timeout=10, retry_sleep=1):
    if not retry:
        return mpc.Client(addr, "AF_INET", None)
    else:
        start_time = time.time()
        while True:
            curr_time = time.time()
            time_delta = curr_time - start_time
            if time_delta >= retry_timeout:
                raise TimeoutError()
            try:
                return mpc.Client(addr, "AF_INET", None)
            except Exception as e:
                if retry_timeout - time_delta <= retry_sleep:
                    raise TimeoutError()
                else:
                    time.sleep(retry_sleep)


class SharedCounter:
    def __init__(self, init=0):
        self.c = mp.Value(c_int64)
        self.c.value = init

    def inc(self, by=1):
        self.c.value += by

    def dec(self, by=1):
        self.c.value -= by

    def value(self):
        return self.c.value

    def set(self, v):
        self.c.value = v

class SharedBuffer:
    def __init__(self):
        self.queue = mp.Queue()

    def empty(self):
        return self.queue.empty()

    def enqueue(self, *items):
        for item in items:
            self.queue.put(item, block=False)
        
    def dequeue(self, block=False):
        return self.queue.get(block)

    def close(self):
        self.queue.close()