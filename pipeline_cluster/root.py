from pipeline_cluster import node
import threading
import queue




# TODO implement all function asyncron that clients can be managed parallel
class Root:
    def __init__(self, *node_addrs):
        self.node_clients = [node.Client(addr) for addr in node_addrs]

        self.input_queue = queue.Queue()
        self.queue_count = 0
        self.is_reset = True
        self.scheduler_state_cond = threading.Condition()


    def setup(self, name, version, tasks):
        for cli in self.node_clients:
            try:
                cli.send_command_setup(name, version, tasks)
            except TimeoutError:
                raise TimeoutError("could not connect to node " + cli.addr[0] + ":" + str(cli.addr[1]))

            except Exception as e:
                raise e
    

    def status(self):
        status = []
        for cli in self.node_clients:
            status.append(cli.send_command_status())
        return status

    def boot(self, output_handler=lambda item: None):
        with self.scheduler_state_cond:
            self.is_reset = False

        for cli in self.node_clients:
            try:
                cli.send_command_boot(n_worker=None) # boot pipeline with #worker = #cores
                cli.send_command_stream_output(output_handler, detach=True)
                threading.Thread(target=self._client_scheduler_routine, args=(cli,), daemon=True).start()
            except Exception as e:
                raise e


    def _client_scheduler_routine(self, cli):
        while True:
            reset, n_idle = cli.send_command_wait_idle()
            if reset:
                return

            if n_idle == 0:
                continue
            
            new_items = []
            with self.scheduler_state_cond:
                while self.queue_count == 0 and not self.is_reset:
                    self.scheduler_state_cond.wait()

                if self.is_reset:
                    return

                for _ in range(n_idle):
                    try:
                        new_items.append(self.input_queue.get_nowait())
                    except queue.Empty:
                        break
                
                self.queue_count -= len(new_items)
                self.scheduler_state_cond.notify_all()

                cli.send_command_feed(*new_items)


    def reset(self):
        for cli in self.node_clients:
            try:
                cli.send_command_reset()
            except Exception as e:
                raise e
        
        with self.scheduler_state_cond:
            self.is_reset = True
            self.scheduler_state_cond.notify_all()

    def wakeup(self):
        for cli in self.node_clients:
            cli.send_command_wakeup()

    def sleep(self):
        for cli in self.node_clients:
            cli.send_command_sleep()

        for cli in self.node_clients:
            cli.send_command_wait_asleep()


    def feed(self, *items):
        with self.scheduler_state_cond:
            for item in items:
                self.input_queue.put(item)
            self.queue_count += len(items)
            self.scheduler_state_cond.notify_all()


    def wait_input(self):
        with self.scheduler_state_cond:
            while self.queue_count > 0 and not self.is_reset:
                self.scheduler_state_cond.wait()

            if self.is_reset:
                return False
            
            return True

    def wait_empty(self):
        with self.scheduler_state_cond:
            while self.queue_count > 0 and not self.is_reset:
                self.scheduler_state_cond.wait()

        if self.is_reset:
            return False

        for cli in self.node_clients:
            cli.send_command_wait_empty()

        return True
