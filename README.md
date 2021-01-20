# pipeline-cluster

- A simple cluster implementation to execute pipelines.
- A pipeline consists of consecutive tasks (taskchain).
- Each task has exactly one input and one or more output items. 
- A task with multiple output items is marked as generator and returns a set of items. 
- Return nothing to drop the item.
- Individual taskchain executions should not depend on each other.

## installation 

```
git clone https://github.com/LessiKai/pipeline_cluster.git
cd pipeline_cluster
pip install --user .

# run a simple test
python simple_cluster_test.py
```

## example usage
- ### run a log server
    ```
    import pipeline_cluster.multiprocess_logging as mpl

    server_addr = ("127.127.127.127", 5555)
    filename = "/tmp/log.txt"
    mpl.serve(server_addr, filename)
    ```

- ### log 
    ```
    import pipeline_cluster.multiprocess_logging as mpl

    log_server_addr = ("127.127.127.127, 5555)
    mpl.configure(log_server_addr)

    mpl.log("hello world!")
    mpl.log("hello world!")
    ```

- ### run a node server
    ```
    import pipeline_cluster.node
    
    log_server_addr = ("127.127.127.127", 5555)
    server_addr = ("127.127.127.127", 5600)

    pipeline_cluster.node.Server(server_addr, log_server_addr).serve()
    ```

- ### run the root node
    ```
    import pipeline_cluster.root

    node_addrs = [("127.127.127.127, 5600)]
    cluster = pipeline_cluster.root.Root(*node_addrs)
    ```

- ### create a simple taskchain
    ```
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
            "is_generator" = True
        },
        {
            "function": wait_more,
            "is_generator": False
        }
    ]
    ```

- ### import taskchain into cluster
    ```
    # run root node

    name = "example pipeline"
    version = 1.0
    cluster.setup(name, version, taskchain)
    ```

- ### start processing
    ```
    # run root node
    # import taskchain into cluster

    # start node workers, input scheduler and output stream
    cluster.boot()

    # feed input
    cluster.feed("hello world!", "hello world!")
    
    # wait for all items beeing processed
    cluster.wait_empty()

    # terminate all workers and reset the taskchain
    # also stops the scheduler and output stream
    cluster.reset()
    ```

A single node cluster example is implemented in simple_cluster_test.py.


## bugs
- on systems other than linux a warning is generated on shutdown, informing about leaked semaphores. This can be ignored. [bug report](https://bugs.python.org/issue38119)
- creating a taskchain in the python shell can produce errors due to the multiprocessing lib. This should be avoided and wont be fixed because it's the intended behaviour. [bug report](https://bugs.python.org/issue25053), [stackoverflow](https://stackoverflow.com/questions/41385708/multiprocessing-example-giving-attributeerror)