from pipeline_cluster import PipelineCluster
from pipeline_cluster_node import PipelineClusterNode
import multiprocess_logging as mpl
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
    PipelineClusterNode(addr, logger_addr, conn_buffer_size=2).serve()

if __name__ == "__main__":
    mpl.serve(logger_addr, logger_file, conn_buffer_size=4, detach=True)

    node_addrs = [("", 5600), ("", 5601)]
    nodes = [mp.Process(target=node_routine, args=(addr, )) for addr in node_addrs]
    for node in nodes:
        node.start()

    cluster = PipelineCluster(*node_addrs)
    cluster.setup("example_pipeline", 1.0, pipeline_tasks)
    print(json.dumps(cluster.status(), indent=4))
    cluster.boot()
    cluster.schedule()
    cluster.feed(True, True, True, True, True)
    cluster.wait_empty()
    print(json.dumps(cluster.status(), indent=4))
    cluster.reset()

    for node in nodes:
        node.terminate()
    for node in nodes:
        node.join()
        