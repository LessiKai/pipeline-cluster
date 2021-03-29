from pipeline_cluster import root, node
import pipeline_cluster.multiprocess_logging as mpl
import multiprocessing as mp
import ghminer.tasks


taskchain = [
    {
        "function": ghminer.tasks.simple_request,
        "is_generator": False
    }
]

if __name__ == "__main__":
    mpl.configure(("localhost", 5555))

    r = root.Root()
    r.search_nodes(verbose=True)
    r.setup("example_pipeline", 1.0, taskchain, local_packages=["../ghminer"], remote_packages=["requests"])
    r.feed(["https://api.myip.com"])
    r.wait_empty()
    r.reset()