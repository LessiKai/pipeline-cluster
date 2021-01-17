from collections import namedtuple
import inspect
import multiprocessing as mp
import multiprocess_logging as mpl
from util import SharedCounter, SharedBuffer
import signal

Task = namedtuple("Task", ["function", "name", "is_generator", "input_buffer", "output_buffer"])



def _worker_routine(taskchain, log_addr, new_items_counter, idle_counter, sleep_counter, terminate_counter, state_cond):
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

    mpl.configure(log_addr)

    while True:
        with state_cond:
            if terminate_counter.value() == 1:
                mpl.log("worker terminated")
                exit(0)

            if sleep_counter.value() == 1 or new_items_counter.value() == 0:
                idle_counter.inc()
                state_cond.notify_all()

                while True:
                    state_cond.wait()
                    if terminate_counter.value() == 1:
                        mpl.log("worker terminated")
                        exit(0)

                    if not sleep_counter.value() == 1 and new_items_counter.value() != 0:
                        break

                idle_counter.dec()

        for i in reversed(range(len(taskchain))):
            curr_task = taskchain[i]
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
                item = curr_task.function(item)
                if item is None:
                    break

                is_last_task = i == len(taskchain) - 1
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
                            mpl.log("worker terminated")
                            exit(1)

                    curr_task = taskchain[j]
                    item = curr_task.function(item)
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
    def __init__(self, log_addr, version=1.0, name="pipeline"):
        self.version = version
        self.log_addr = log_addr
        self.taskchain = []
        self.worker = []

        self.new_items_counter = SharedCounter()
        self.idle_counter = SharedCounter()
        self.sleep_counter = SharedCounter()
        self.terminate_counter = SharedCounter()
        self.state_cond = mp.Condition()


    def add_task(self, *tasks, is_generator=False):
        """
        Adds a task into the current pipeline taskchain.
        A task is a function with one input parameter and one pickable ouput item.
        In case of a generator task, the output is a set of pickable output items.
        In both cases, if None is returned (or nothing) no item is forwarded.
        Following buffers are added:    taskchain input buffer, 
                                        succeeding buffer for each generator function, 
                                        taskchain output buffer
        """
        for t in tasks:
            assert inspect.isfunction(t)
            n_params = len(inspect.signature(t).parameters)
            assert n_params == 1

            input_buffer = None
            if self.taskchain:
                prev_task = self.taskchain[-1]
                if not prev_task.is_generator:
                    self.taskchain[-1] = Task(prev_task.function, prev_task.name, prev_task.is_generator, prev_task.input_buffer, None)
                else:
                    input_buffer = prev_task.output_buffer
            else:
                input_buffer = SharedBuffer()

            self.taskchain.append(Task(t, t.__name__, is_generator, input_buffer, SharedBuffer()))


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

    def boot(self, n_worker=mp.cpu_count()):
        """
        Starts the pipeline workers.
        Should only be called at the start or after a reset.
        """
        assert self.is_reset()

        self.worker = [mp.Process(target=_worker_routine, args=(self.taskchain, self.log_addr, self.new_items_counter, self.idle_counter, self.sleep_counter, self.terminate_counter, self.state_cond), daemon=True) for _ in range(n_worker)]
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
        assert self.is_running()

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


    def feed(self, *items):
        """
        Feed input items into the input buffer.
        """
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

    def wait_output(self):
        """
        Wait for output items.
        This function should only be called on a running pipeline.
        As soon as the pipeline sleeps or is reset, this function returns immediately. (Return False)
        Otherwise (Return True)
        """
        with self.state_cond:
            while self.is_running() and self.is_awake() and self.taskchain[-1].output_buffer.empty():
                self.state_cond.wait()

            if not self.is_running() or not self.is_awake():
                return False
            return True

    def wait_idle(self):
        """
        Wait until one worker is idle. (Return True)
        Returns immediately when the pipeline is asleep or reset (Return False)
        """
        with self.state_cond:
            while self.is_running() and self.is_awake() and self.idle_counter.value() == 0:
                self.state_cond.wait()

            if not self.is_running() or not self.is_awake():
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