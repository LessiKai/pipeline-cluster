import pipeline_cluster.pipeline
import pipeline_cluster.multiprocess_logging as mpl


class Dummy:
    def __init__(self, message):
        self.message = message

    def __call__(self, item):
        mpl.log(self.message)
        return item

def dummy(item):
    mpl.log("dummy message")
    return item

taskchain = [
    (Dummy, ("some class message", ), False),
    (dummy, set(), False)
]

if __name__ == "__main__":
    mpl.serve(("", 5555), "log.txt", detach=True)
    pl = pipeline_cluster.pipeline.Pipeline(("", 5555), taskchain=taskchain)

    pl.boot(n_workers=1)
    pl.feed(["some", "some", "some"])
    pl.wait_empty()
    pl.reset()
    