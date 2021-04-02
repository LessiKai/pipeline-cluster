from pipeline_cluster import root, node
import pipeline_cluster.multiprocess_logging as mpl
import multiprocessing as mp
import json
import time
import sys
from test_tasks import tasks

taskchain = [
    {
        "function": tasks.wait,
        "is_generator": True
    },
    {
        "function": tasks.wait_more,
        "is_generator": False
    }
]



if __name__ == "__main__":
    r = root.Root()
    if not r.search_nodes(network="127.0.0.0/24", port=6000, verbose=True):
        print("node nodes are running... exit")
        
    r.setup("example_pipeline", 1.1, taskchain, local_packages=["test_tasks"])
    r.feed(["hello world", "second hello world"])
    r.wait_empty()
    r.reset()
