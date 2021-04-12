import multiprocessing as mp
import multiprocessing.connection as mpc
import time
from ctypes import c_int64
import os

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
                    raise TimeoutError("could not connect to " + addr[0] + ":" + str(addr[1]))
                else:
                    time.sleep(retry_sleep)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def str_addr(addr):
    return addr[0] + ":" + str(addr[1])


def dict_to_dir(root, dir_dict):
    if not os.path.isdir(root):
        os.mkdir(root)

    for key, value in dir_dict.items():
        if isinstance(value, dict):
            dict_to_dir(os.path.join(root, key), value)
        
        else:
            with open(os.path.join(root, key), "wb") as fd:
                fd.write(value)

def dir_to_dict(root, ignore=[".venv", "__pycache__", ".git"]):
    root = os.path.abspath(root)
    result = {}
    result[os.path.basename(root)] = _dir_to_dict(root, ignore)
    return result

def _dir_to_dict(root, ignore):
    result = {}
    for entry in os.listdir(root):
        if entry in ignore:
            continue

        full_path = os.path.join(root, entry)
        if os.path.isdir(full_path):
            result[entry] = _dir_to_dict(full_path, ignore)
        
        else:
            with open(full_path, "rb") as fd:
                result[entry] = fd.read()
    
    return result


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