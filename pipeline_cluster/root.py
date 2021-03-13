from pipeline_cluster import node
import threading
import queue
import ipaddress
import pipeline_cluster.multiprocess_logging as mpl
import pipeline_cluster.util
import multiprocessing as mp




# TODO implement all function asyncron that clients can be managed parallel
class Root:
    def __init__(self, node_addrs=[]):
        self.node_clients = []

        self.input_queue = queue.Queue()
        self.queue_count = 0
        self.is_reset = True
        self.scheduler_state_cond = threading.Condition()

        self.add_nodes(node_addrs)

    def search_nodes(self, network="127.0.0.0/24", port=6000, verbose=False):
        with self.scheduler_state_cond:
            if not self.is_reset:
                raise RuntimeError("The pipeline-cluster has to be reset to be able to add more nodes")

        network = ipaddress.ip_network(network, strict=False)
        if verbose:
            mpl.log("scanning network " + str(network) + " for nodes on port " + str(port) + " (" + str(network.num_addresses) + " hosts)")
        
        thrs = []
        node_client_queue = queue.Queue()
        for addrs_chunk in pipeline_cluster.util.chunks([(str(h), port) for h in network if (str(h), port) not in [n.addr for n in self.node_clients]], network.num_addresses // mp.cpu_count() + 1):
            thr = threading.Thread(target=Root._check_nodes, args=(addrs_chunk, node_client_queue, verbose))
            thrs.append(thr)
            thr.start()
        
        for thr in thrs:
            thr.join()

        while True:
            try:
                self.node_clients.append(node_client_queue.get_nowait())
            except:
                break

        if verbose:
            mpl.log("finished scanning network")

    @staticmethod
    def _check_nodes(node_addrs, output_queue, verbose=False):
        for addr in node_addrs:
            node_client = node.Client(addr)
            try:
                node_status = node_client.send_command_status(retry=False)
            except:
                continue
            
            if not node_status or not node_status["running"]:
                if verbose:
                    mpl.log("node found at " + pipeline_cluster.util.str_addr(addr))
                output_queue.put(node_client)
            else:
                raise RuntimeError("node at " + pipeline_cluster.util.str_addr(addr) + " is not reset. Is there another root running?\nThe pipeline-cluster only supports one root at a time.")

    def add_node(self, addr):
        with self.scheduler_state_cond:
            if not self.is_reset:
                raise RuntimeError("The pipeline-cluster has to be reset to be able to add more nodes")

        node_client = node.Client(addr)
        node_status = node_client.send_command_status(timeout=5, retry_sleep=1)
        if not node_status or not node_status["running"]:
            self.node_clients.append(node_client)
        else:
            raise RuntimeError("The node at " + pipeline_cluster.util.str_addr(addr) + " is not reset. Is there another root running?\nThe pipeline-cluster only supports one root at a time.")

    def add_nodes(self, node_addrs):
        for addr in node_addrs:
            self.add_node(addr)


    def setup(self, name, version, tasks):
        for cli in self.node_clients:
            cli.send_command_setup(name, version, tasks)
            
    

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


    def feed(self, items):
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
