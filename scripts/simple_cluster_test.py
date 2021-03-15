from pipeline_cluster import root, node
import pipeline_cluster.multiprocess_logging as mpl
import multiprocessing as mp
import json
import time


log_server_addr = ("", 5000)
log_file = "log.txt"

import time
def wait(item):
    mpl.log(item)
    time.sleep(2)
    return (item, item)

def wait_more(item):
    mpl.log(item)
    time.sleep(4)
    return item

taskchain = [
    {
        "function": wait,
        "is_generator": True
    },
    {
        "function": wait_more,
        "is_generator": False
    }
]


def node_routine(addr):
    node.Server(addr, log_server_addr, conn_buffer_size=2).serve()

if __name__ == "__main__":
    mpl.serve(log_server_addr, log_file, conn_buffer_size=2, detach=True)
    mpl.configure(log_server_addr)

    node_addrs = [("localhost", 5600), ("localhost", 5601)]
    nodes = [mp.Process(target=node_routine, args=(addr, )) for addr in node_addrs]
    for n in nodes:
        n.start()

    r = root.Root(node_addrs)
    r.setup("example_pipeline", 1.0, taskchain, output_handler=lambda item: print("output: " + item))
    r.feed(["hello world!", "its me an input item!"])
    r.wait_empty()
    r.reset()

    r = root.Root()
    r.search_nodes(network="127.0.0.0/24", port=5600, verbose=True)
    r.add_node(("localhost", 5601))
    r.setup("example_pipeline", 1.1, taskchain)
    r.feed(["hello world", "second hello world"])
    r.wait_empty()
    r.reset()


    for node in nodes:
        node.terminate()
    for node in nodes:
        node.join()