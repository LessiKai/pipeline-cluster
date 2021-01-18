from pipeline_cluster import node
import threading
import queue




# TODO implement all function asyncron that clients can be managed parallel
class Root:
    def __init__(self, *node_addrs):
        self.node_clients = [node.Client(addr) for addr in node_addrs]

        self.input_queue = queue.Queue()
        self.queue_count = 0
        self.no_more_input = False
        self.asleep = False
        self.scheduler_state_cond = threading.Condition()


    def setup(self, name, version, tasks):
        for cli in self.node_clients:
            try:
                cli.send_command_setup(name, version, tasks)
            except Exception as e:
                raise e
    

    def status(self):
        status = []
        for cli in self.node_clients:
            status.append(cli.send_command_status())
        return status

    def boot(self):
        for cli in self.node_clients:
            try:
                cli.send_command_boot(n_worker=None) # boot pipeline with #worker = #cores
            except Exception as e:
                raise e

    def reset(self):
        for cli in self.node_clients:
            try:
                cli.send_command_reset()
            except Exception as e:
                raise e

    def wakeup(self):
        for cli in self.node_clients:
            cli.send_command_wakeup()

    def sleep(self):
        for cli in self.node_clients:
            cli.send_command_sleep()

        for cli in self.node_clients:
            cli.send_command_wait_asleep()
        
        with self.scheduler_state_cond:
            self.asleep = True
            self.scheduler_state_cond.notify_all()


    def feed(self, *items):
        for item in items:
            self.input_queue.put(item)

        with self.scheduler_state_cond:
            self.queue_count += len(items)

    def stream_output(self, output_handler):
        for cli in self.node_clients:
            cli.send_command_stream_output(output_handler, detach=True)

    def _client_scheduler_routine(self, cli):
        while True:
            sleeping, n_idle = cli.send_command_wait_idle()
            if sleeping:
                return

            if n_idle == 0:
                continue
            
            new_items = []
            with self.scheduler_state_cond:
                while self.queue_count == 0 and not self.no_more_input:
                    self.scheduler_state_cond.wait()

                if self.queue_count == 0 and self.no_more_input:
                    return

                for _ in range(n_idle):
                    try:
                        new_items.append(self.input_queue.get_nowait())
                    except queue.Empty:
                        break
                
                self.queue_count -= len(new_items)
                self.scheduler_state_cond.notify_all()

            cli.send_command_feed(*new_items)


    def schedule(self):
        for cli in self.node_clients:
            threading.Thread(target=self._client_scheduler_routine, args=(cli,)).start()

    def wait_input(self):
        with self.scheduler_state_cond:
            while self.queue_count != 0 and not self.asleep:
                self.scheduler_state_cond.wait()

            if self.asleep:
                return False

            return True

    def wait_empty(self):
        with self.scheduler_state_cond:
            self.no_more_input = True
            self.scheduler_state_cond.notify_all()

        for cli in self.node_clients:
            cli.send_command_wait_empty()

        if self.queue_count > 0 or (self.queue_count == 0 and self.asleep):
            return False

        return True
    
