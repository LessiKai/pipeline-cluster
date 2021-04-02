import time
import pipeline_cluster.multiprocess_logging as mpl

def wait(item):
    mpl.log(item)
    time.sleep(2)
    return (item, item)

def wait_more(item):
    mpl.log(item)
    time.sleep(4)
    return item