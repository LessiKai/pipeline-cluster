# pipeline-cluster

- A simple cluster implementation for pipelines.
- A pipeline consists of consecutive tasks (taskchain).
- Each task has exactly one input and one or more output items.
- Individual taskchain executions should not depend on each other.

## installation 

```
git clone https://github.com/LessiKai/pipeline-cluster.git
cd pipeline-cluster
pip install --user .
```

## getting started

___
#### WARNING: 
Only use this cluster in a trusted network! Otherwise other people can abuse it. The cluster is not designed with security in mind.
___

The cluster consists of three essential parts.
- One log server
- One or multiple node servers
- One root node


### log server
Because the cluster nodes are normally spread over multiple hosts, the pipeline-cluster provides a simple logging server implementation.  

The easiest way to start a log server is to use the pcluster script. 
```
pcluster log --host localhost --port 5000 --outfile log.txt
``` 
This will start a log server listening at `localhost` on port `5000`. Afterwards you can log using the python interface.
```
import pipeline_cluster

pipeline_cluster.log("hello world!", addr=("localhost", 5000))
```
Use `set_default_log_address(("localhost", 5000))` to prevent typing the address for each log.


### node server
The node servers are the actual heart of the cluster. Each host should only run one server at a time (each additional server on one host would just add more overhead overall and would not benefit performance except heavy IO in rare cases). A node server is responsible for recieving input data, processing it and returning output to the client.  

A node server should only be started via the pcluster script 
```
pcluster node --host localhost --port 6000 --log-host [...] --log-port [...]
``` 
This will start a server listening at `localhost` on port `6000` and logs to the provided log server. It is recommended to start a node in a virtual environment, because when configuring pipelines, missing packages are installed on the fly and could blur up the locally installed pip environment. 

### root node
Each node can be controlled individually by a respective client. To make the process easier the root node handles all of them together. Thus the root node is the client for the whole cluster. A cluster should only have one root node running at a time. It can only be created via the python interface.
```
import pipeline_cluster.root

node_addrs = [("localhost", 6000), ("localhost", 6001)]
cluster = pipeline_cluster.root.Root(node_addrs)
```
A more simple and flexible way of configuring the cluster nodes for the root node is to just search them in a network.
```
import pipeline_cluster.root

cluster = pipeline_cluster.root.Root()
cluster.search_nodes(network="123.123.123.0/24", port=6000)
cluster.search_nodes(network="123.123.123.0/24", port=6001)
```
Each call to `search_nodes` scans the provided network and adds all found notes on given port  to the already known ones.

The next step is to configure the pipeline that should be executed. Therefore a taskchain has to be defined. A taskchain is a list of functions or callable classes that are consecutively executed. To be able to copy those tasks onto the node machines, they have to be included in a package.
```
import pipeline_cluster.multiprocess_logging as mpl
def simple_task(item):
    mpl.log(item)
    return item

def generator_task(item):
    mpl.log(item)
    return (item, item)

class DropItem:
    def __init__(self, drop_message):
        self.drop_message = drop_message
        
    def __call__(self, item):
        mpl.log(self.drop_message)
        return None
```
As you can see it is possible to return one or more output items. To drop an item, just return nothing or None. The log address is preconfigured when a task is executed.  

After defining the tasks, the taskchain schema has to be setup. Therefor a list of tasks is created, each object defining one. Tasks that return multiple items are marked as generator. Callable classes have an extra args field to construct them once per worker.
```
taskchain = [
    {
        "function": simple_task,
        "is_generator": False
    },
    {
        "function": generator_task,
        "is_generator": True
    },
    {
        "function": DropItem,
        "is_generator": False,
        "args": ("item dropped", )
    }
]
``` 
 The cluster is then configured using a name, version and the taskchain, as well as the packages needed for the tasks to run. Additionally an output routine can be added, which is executed locally on the root host for each output item. This starts the worker processes of each node and the input scheduler of the root node.  
```
cluster.setup("example-pipeline", 1.0, taskchain, local_packages=["task_package"], output_handler=lambda output_item: print(output_item))
```
Its time to feed some input and wait until all items are processed.
```
cluster.feed("hello world!", "its me an input item!")
cluster.wait_empty()
```
Afterwards always kill all workers and background threads!  
```
cluster.reset()
```
It is also possible to wait until one worker idles because there is no more input to process.  
```
cluster.wait_input()
```
This can be applied to an input loop.
```
while more_data_available:
    cluster.wait_input()
    cluster.feed(get_next_input())
cluster.wait_empty()
```

Please, you are welcome to message me if you have any questions! :blush:  


## bugs
- on systems other than linux a warning is generated on shutdown, informing about leaked semaphores. This can be ignored. [bug report](https://bugs.python.org/issue38119)
