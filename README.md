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

The easiest way to start a log server is to use the plc_log.py script. 
```
python3 plc_log.py --interface localhost --port 5000 --file log.txt
``` 
This will start a log server listening at `localhost` on port `5000`. The same server can be started using the python interface.  
```
import pipeline_cluster.multiprocess_logging as mpl

server_addr = ("localhost", 5000)
filename = "log.txt"
mpl.serve(server_addr, filename)
```
To log something, the log client has to be configured once per process. Afterwards one can log with `log(msg)`. It is safe to log from different threads.
```
import pipeline_cluster.multiprocess_logging as mpl

log_server_addr = ("localhost", 5000)
mpl.configure(log_server_addr)

mpl.log("hello world!")
mpl.log("hello world!")
```


### node server
The node servers are the actual heart of the cluster. Each host should only run one server at a time. A node server is responsible for recieving input data, processing it and returning output to the client.  

The easiest way to start a node server is to use the plc_node.py script.  
```
python3 plc_node.py --interface localhost --port 6000 --log-interface [...] --log-port [...]
``` 
This will start a server listening at `localhost` on port `6000` and logs to the provided log server. The same server can be started using the python interface.
```
import pipeline_cluster.node

log_server_addr = ("localhost", 6000)
server_addr = ("localhost", 5000)

pipeline_cluster.node.Server(server_addr, log_server_addr).serve()
```

### root node
Each node can be controlled individually by a respective client. To make the process easier the root node handles all of them together. Thus the root node is the client for the whole cluster. It can only be created via the python interface.
```
import pipeline_cluster.root

node_addrs = [("localhost", 6000), ("localhost", 6001)]
cluster = pipeline_cluster.root.Root(*node_addrs)
```
The next step is to configure the pipeline that should be executed. This is done by defining a taskchain. Each task is one step in the pipeline and has to have one input parameter for the input item. There are two types of tasks. Those which return one item and those that return multiple. If a task returns mutliple items it has to be marked as generator.
```
import pipeline_cluster.multiprocess_logging as mpl
def simple_task(item):
    mpl.log(item)
    return item

def generator_task(item):
    mpl.log(item)
    return (item, item)

def drop_item(item):
    mpl.log(item)
    return None # or just return nothing

taskchain = [
    {
        "function": simple_task,
        "is_generator": False
    },
    {
        "function": generator_task,
        "is_generator": True
    }
]
``` 
As one can see the logging client is already preconfigured when the task is executed on a node. The cluster can then be configured using a name, version and the taskchain.  
```
cluster.setup("example-pipeline", 1.0, taskchain)
```
The next thing to do is to boot the cluster. This starts the worker proceses of each node and the input scheduler of the root node. Also at this point an output routine can be defined which will be run locally on the root host for each output.
```
cluster.boot(output_handler=lambda output_item: print(output_item))
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
The script `simple_cluster_test.py` contains a single machine, two node cluster test. If it fails, check your firewall configuration or create an issue.  

Please, you are welcome to message me if you have a question! :blush:  


## bugs
- on systems other than linux a warning is generated on shutdown, informing about leaked semaphores. This can be ignored. [bug report](https://bugs.python.org/issue38119)
- creating a taskchain with functions in a not importable module can produce errors due to the multiprocessing lib (e.g. the python shell). This should be avoided and wont be fixed because it's the intended behaviour. [bug report](https://bugs.python.org/issue25053), [stackoverflow](https://stackoverflow.com/questions/41385708/multiprocessing-example-giving-attributeerror)
