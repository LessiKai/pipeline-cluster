from pipeline_cluster import root, node
import pipeline_cluster.multiprocess_logging as mpl
import multiprocessing as mp
import json


logger_addr = ("", 5555)
logger_file = "log.txt"

import time
def wait(item):
    mpl.log("waiting...")
    time.sleep(2)
    return item

def wait_more(item):
    mpl.log("waiting more...")
    time.sleep(4)
    return item

pipeline_tasks = [
    {
        "function": wait,
        "is_generator": False
    },
    {
        "function": wait_more,
        "is_generator": False
    }
]


def node_routine(addr):
    node.Server(addr, logger_addr, conn_buffer_size=2).serve()

if __name__ == "__main__":
    mpl.serve(logger_addr, logger_file, conn_buffer_size=4, detach=True)

    node_addrs = [("", 5600), ("", 5601)]
    nodes = [mp.Process(target=node_routine, args=(addr, )) for addr in node_addrs]
    for node in nodes:
        node.start()

    root = root.Root(*node_addrs)
    root.setup("example_pipeline", 1.0, pipeline_tasks)
    print(json.dumps(root.status(), indent=4))
    root.boot()
    root.schedule()
    root.feed(True, True, True, True, True)
    root.wait_empty()
    print(json.dumps(root.status(), indent=4))
    root.reset()

    for node in nodes:
        node.terminate()
    for node in nodes:
        node.join()
        