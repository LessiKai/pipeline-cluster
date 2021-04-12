from collections import namedtuple
import inspect
import signal
import multiprocessing as mp
import pipeline_cluster.multiprocess_logging as mpl
from pipeline_cluster import util
import time
import traceback
import os
import json
import time

Task = namedtuple("Task", ["function", "name", "args", "is_generator", "is_class", "input_buffer", "output_buffer"])


def _worker_signal_handler(signum, frame):
    print(signum)
    mpl.log("worker terminated")
    exit(1)

def _worker_routine(taskchain, log_addr, new_items_counter, idle_counter, sleep_counter, terminate_counter, state_cond, benchmark_folder):
    """
    The worker routine for pipeline workers.
    Workers feed items in deepest first order.
    They passively wait for input if no more items are available.
    When the sleep_counter is set, the workers finish the current task and sleep until the counter is unset,
    even if there are items to process.
    """

    # workers should only be terminated with the terminate counter from the root pipeline process
    # thus signals get ignored
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    # TODO: ignore/handle more signals if needed

    mpl.set_default_address(log_addr)
    benchmark_file = os.path.join(benchmark_folder, str(os.getpid()))
    benchmark = [{
        "task": t.name,
        "processed": 0,
        "time": 0
    } for t in taskchain]

    # prepare classes taskchain
    taskchain = [Task(t.function(*t.args), *t[1:]) if t.is_class else t for t in taskchain]

    while True:
        with open(benchmark_file, "w") as fd:
            json.dump(benchmark, fd)

        with state_cond:
            if terminate_counter.value() == 1:
                exit(0)

            if sleep_counter.value() == 1 or new_items_counter.value() == 0:
                idle_counter.inc()
                state_cond.notify_all()

                while True:
                    state_cond.wait()
                    if terminate_counter.value() == 1:
                        exit(0)

                    if not sleep_counter.value() == 1 and new_items_counter.value() != 0:
                        break

                idle_counter.dec()

        for i in reversed(range(len(taskchain))):
            curr_task = taskchain[i]
            curr_benchmark = benchmark[i]
            if curr_task.input_buffer is not None and not curr_task.input_buffer.empty():
                try:
                    item = curr_task.input_buffer.dequeue()
                except:
                    # at this point the buffer recently had at least one item
                    # because there is none currently another another process removed one right before
                    # because of the semaphore counter there must be another item in a task before this
                    # thus continue not break
                    continue 

                with state_cond:
                    new_items_counter.dec()

                # handle first item
                start_time = time.time()
                try:
                    item = curr_task.function(item)
                except Exception:
                    mpl.log("Item dropped due to exception:\n" + traceback.format_exc())
                    break
                d_time = time.time() - start_time
                curr_benchmark["time"] += d_time
                curr_benchmark["processed"] += 1

                if item is None:
                    break

                is_last_task = (i == len(taskchain) - 1)
                if curr_task.output_buffer is not None:
                    if curr_task.is_generator:
                        if not item:
                            break
                        curr_task.output_buffer.enqueue(*item)
                        if not is_last_task:
                            with state_cond:
                                new_items_counter.inc(len(item))
                                state_cond.notify_all()
                        else:
                            with state_cond:
                                state_cond.notify_all()
                    else:
                        curr_task.output_buffer.enqueue(item)
                        if not is_last_task:
                            with state_cond:
                                new_items_counter.inc()
                                state_cond.notify_all()
                        else:
                            with state_cond:
                                state_cond.notify_all()
                    break

                # feed forward
                for j in range(i + 1, len(taskchain)):
                    with state_cond:
                        if terminate_counter.value() == 1:
                            exit(0)

                    curr_task = taskchain[j]
                    curr_benchmark = benchmark[j]
                    
                    start_time = time.time()
                    try:
                        item = curr_task.function(item)
                    except Exception:
                        mpl.log("Item dropped due to exception:\n" + traceback.format_exc())
                        break
                    d_time = time.time() - start_time
                    curr_benchmark["time"] += d_time
                    curr_benchmark["processed"] += 1

                    if item is None:
                        break
                    is_last_task = j == len(taskchain) - 1
                    if curr_task.output_buffer is not None:
                        if curr_task.is_generator:
                            if not item:
                                break
                            curr_task.output_buffer.enqueue(*item)
                            if not is_last_task:
                                with state_cond:
                                    new_items_counter.inc(len(item))
                                    state_cond.notify_all()
                            else:
                                with state_cond:
                                    state_cond.notify_all()
                        else:
                            curr_task.output_buffer.enqueue(item)
                            if not is_last_task:
                                with state_cond:
                                    new_items_counter.inc()
                                    state_cond.notify_all()
                            else:
                                with state_cond:
                                    state_cond.notify_all()
                        break
                
                break


class Pipeline:
    def __init__(self, log_addr, version=1.0, name="pipeline", taskchain=[], benchmark_folder="/tmp/pipeline-cluster-benchmarks"):
        self.version = version
        self.name = name
        self.log_addr = log_addr
        self.taskchain = []
        self.worker = []
        self.benchmark_folder = benchmark_folder

        self.new_items_counter = util.SharedCounter()
        self.idle_counter = util.SharedCounter()
        self.sleep_counter = util.SharedCounter()
        self.terminate_counter = util.SharedCounter()
        self.state_cond = mp.Condition()

        if not os.path.isdir(self.benchmark_folder):
            os.mkdir(self.benchmark_folder)

        for task in taskchain:
            self._add_task(task[0], args=task[1], is_generator=task[2])

    def _add_task(self, task, args=set(), is_generator=False):
        """
        Adds a task into the current pipeline taskchain.
        A task is a function with one input parameter and one pickable ouput item.
        In case of a generator task, the output is a set of pickable output items.
        In both cases, if None is returned (or nothing) no item is forwarded.
        Following buffers are added:    taskchain input buffer, 
                                        succeeding buffer for each generator function, 
                                        taskchain output buffer
        """
        assert inspect.isfunction(task) or inspect.isclass(task), "The added task has to be of function or class type"
        n_params = len(inspect.signature(task).parameters) if inspect.isfunction(task) else len(inspect.signature(task.__call__).parameters)
        if inspect.isfunction(task):
            assert n_params == 1, "Tasks of type function should have exactly one input parameter for the input item"
        else:
            assert n_params == 2, "Tasks of class type should have exactly two parameters, one of them the class reference and one for the input item"

        input_buffer = None
        if self.taskchain:
            prev_task = self.taskchain[-1]
            if not prev_task.is_generator:
                self.taskchain[-1] = Task(prev_task.function, prev_task.name, prev_task.args, prev_task.is_generator, prev_task.is_class, prev_task.input_buffer, None)
            else:
                input_buffer = prev_task.output_buffer
        else:
            input_buffer = util.SharedBuffer()
        self.taskchain.append(Task(task, task.__name__, args, is_generator, inspect.isclass(task), input_buffer, util.SharedBuffer()))


    def __str__(self):
        taskchain_str = "=>[b]"
        for t in self.taskchain:
            taskchain_str += "->" + t.name
            if t.is_generator:
                taskchain_str += "=>[b]"
            elif t.output_buffer is not None:
                taskchain_str += "->[b]"
        return taskchain_str


    def get_version(self):
        return self.version

    def get_name(self):
        return self.name

    def get_n_idle(self):
        """
        Get the number of idle workers.
        """
        return self.idle_counter.value()

    def get_n_worker(self):
        """
        Get the number of workers in total.
        """
        return len(self.worker)

    def boot(self, n_workers=mp.cpu_count()):
        """
        Starts the pipeline workers.
        Should only be called at the start or after a reset.
        """
        assert self.is_reset()

        # setup benchmark folder for specific boot
        last_benchmark_folders = [os.path.join(self.benchmark_folder, d) for d in os.listdir(self.benchmark_folder) if os.path.isdir(os.path.join(self.benchmark_folder, d))]
        last_benchmark_id = max([int(os.path.basename(d)) for d in last_benchmark_folders], default=0)
        
        benchmark_id = last_benchmark_id + 1 
        curr_benchmark_folder = os.path.join(self.benchmark_folder, str(benchmark_id))
        os.mkdir(curr_benchmark_folder)

        self.worker = [mp.Process(target=_worker_routine, args=(self.taskchain, self.log_addr, self.new_items_counter, self.idle_counter, self.sleep_counter, self.terminate_counter, self.state_cond, curr_benchmark_folder), daemon=True) for _ in range(n_workers)]
        for w in self.worker:
            w.start()


    def reset(self):
        """
        Resets the pipeline. 
        Workers are terminated regardless of what they do.
        This can result in lock compromise such that the taskchain has to be rebuild.
        At the moment this has to be done manually.
        TODO: implement automatic taskchain rebuild
        To get a controlled shutdown, first set the pipeline asleep and wait for all workers to finish, then reset.
        """
        if not self.is_running():
            return

        with self.state_cond:
            self.terminate_counter.set(1)
            self.state_cond.notify_all()
        
        for w in self.worker:
            w.join()

        with self.state_cond:
            self.worker = []
            self.idle_counter.set(0)
            self.new_items_counter.set(0)
            self.sleep_counter.set(0)
            self.terminate_counter.set(0)
            self.state_cond.notify_all()

        self.taskchain = []


    def feed(self, items):
        """
        Feed input items into the input buffer.
        """
        assert self.is_running(), "The pipeline can only be fed when its running"

        self.taskchain[0].input_buffer.enqueue(*items)
        with self.state_cond:
            self.new_items_counter.inc(len(items))
            self.state_cond.notify_all()

    def get_output(self):
        """
        Query the output buffer for output items.
        This function should only be called on a running pipeline.
        The pipeline can be asleep or awake though.
        To wait until output is awailable use: wait_ouput
        """
        output = []
        while True:
            try:
                output.append(self.taskchain[-1].output_buffer.dequeue(False))
            except:
                return output

    def is_empty(self):
        """
        Check if no more items need to be forwarded and all workers are in idle.
        """
        return self.idle_counter.value() == len(self.worker) and self.new_items_counter.value() == 0

    def wait_output(self, abort_on_sleep=True):
        """
        Wait for output items.
        This function should only be called on a running pipeline.
        As soon as the pipeline is reset or sleeps, this function returns immediately. (Return False)
        Otherwise (Return True)
        """
        with self.state_cond:

            if abort_on_sleep:
                while self.is_running() and self.is_awake() and self.taskchain[-1].output_buffer.empty():
                    self.state_cond.wait()

                if not self.is_running() or not self.is_awake():
                    return False
                return True

            else:
                while self.is_running() and self.taskchain[-1].output_buffer.empty():
                    self.state_cond.wait()

                if not self.is_running():
                    return False
                return True

    def wait_idle(self, abort_on_sleep=True):
        """
        Wait until one worker is idle. (Return True)
        Returns immediately when the pipeline is reset or asleep (Return False)
        """
        with self.state_cond:

            if abort_on_sleep:
                while self.is_running() and self.is_awake() and self.idle_counter.value() == 0:
                    self.state_cond.wait()

                if not self.is_running() or not self.is_awake():
                    return False
                return True

            else:
                while self.is_running() and (self.is_asleep() or self.idle_counter.value() == 0):
                    self.state_cond.wait()

                if not self.is_running():
                    return False
                return True

    def wait_empty(self):
        """
        Wait until the pipeline is empty. (Return True)
        Returns immediately when the pipeline is asleep or reset (Return False)
        """
        with self.state_cond:
            while self.is_running() and self.is_awake() and not self.is_empty():
                self.state_cond.wait()

            if not self.is_running() or not self.is_awake():
                return False
            return True

    def sleep(self):
        """
        Set the pipeline in sleep mode.
        Workers will finish the current task and stop until wakeup is called.
        This can take some time depending on the task.
        To wait until all workers are asleep use: wait_asleep
        """
        with self.state_cond:
            self.sleep_counter.set(1)

    def wait_asleep(self):
        """
        Wait until all workers are asleep or the pipeline is reset.
        """
        with self.state_cond:
            while self.is_running() and self.idle_counter.value() != len(self.worker):
                self.state_cond.wait()

    def wakeup(self):
        """
        Set the pipeline back in awake mode.
        Workers will start immediately.
        """
        with self.state_cond:
            self.sleep_counter.set(0)
            self.state_cond.notify_all()

    def is_awake(self):
        """
        Return if the pipeline is awake(True) or asleep(False)
        """
        return self.sleep_counter.value() == 0

    def is_asleep(self):
        """
        Return if the pipeline is asleep(True) or awake(False)
        """
        return not self.is_awake()

    def is_running(self):
        """
        Return if the pipeline was booted and has workers.
        """
        return len(self.worker) > 0

    def is_reset(self):
        """
        Return if the pipeline was not booted or was reset and has no workers.
        """
        return not self.is_running()