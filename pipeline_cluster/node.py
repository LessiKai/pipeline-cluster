import threading
import multiprocessing as mp
import multiprocessing.connection as mpc
from pipeline_cluster import util
from pipeline_cluster import pipeline
import pipeline_cluster.multiprocess_logging as mpl
import signal
import traceback
import time
import os
import subprocess
import importlib
import sys

class Command:
    ERROR = "ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    SETUP = "SETUP"
    ENVIRONMENT = "ENVIRONMENT"
    STATUS = "STATUS"
    RESET = "RESET"
    WAKEUP = "WAKEUP"
    SLEEP = "SLEEP"
    FEED = "FEED"
    STREAM_OUTPUT = "STREAM_OUTPUT"
    STREAM_END = "STREAM_END"
    WAIT_IDLE = "WAIT_IDLE"
    WAIT_EMPTY = "WAIT_EMPTY"
    WAIT_ASLEEP = "WAIT_ASLEEP"




class Server:
    def __init__(self, addr, log_addr, conn_buffer_size=2, benchmark_folder="/tmp/pipeline-cluster-benchmarks"):
        self.addr = addr
        self.conn_buffer_size = conn_buffer_size
        self.log_addr = log_addr
        self.benchmark_folder = benchmark_folder
        self.pipeline = None
        self.start_time = None

    def _signal_handler(self, signum, frame):
        if self.pipeline is not None:
            self.pipeline.reset()
        exit(0)

    def serve(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.start_time = time.time()
        mpl.log("node server started at " + self.addr[0] + ":" + str(self.addr[1]), self.log_addr)
        with mpc.Listener(self.addr, "AF_INET", self.conn_buffer_size, None) as lst:
            while True:
                conn = lst.accept()
                threading.Thread(target=self._handle_connection, args=(conn, lst.last_accepted), daemon=True).start()

    def _handle_connection(self, conn, caddr):
        while True:
            try:
                req = conn.recv()
                try:
                    response = self._handle_request(conn, req)
                except Exception as e:
                    mpl.log("exception occured during request " + str(req) + "\n" + traceback.format_exc(), addr=self.log_addr)
                    if self.pipeline is not None:
                        mpl.log("reset pipeline")
                        try:
                            self.pipeline.reset()
                            self.pipeline = None
                        except Exception as e:
                            mpl.log("failed to reset pipeline, maybe you have to kill the workers manually", addr=self.log_addr)

                    conn.send({
                        "command": Command.INTERNAL_ERROR,
                        "describtion": "internal error occured"
                    })

                    exit(1)

                conn.send(response)
            except EOFError as e: # maybe this should catch all exceptions in case the client disconnects while sending
                break
            except ConnectionResetError as e:
                break
        
        conn.close()

    def _handle_request(self, conn, req):
        if type(req) != dict:
            return {
                "command": Command.ERROR,
                "describtion": "request has to be of type dict"
            }

        if req.get("node", None) is None:
            return {
                "command": Command.ERROR,
                "describtion": "node field is missing"
            }

        if req.get("command", None) is None:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "command field is missing"
            }

        command = req["command"]
        
        if command == Command.SETUP:
            return self._handle_command_setup(req)

        elif command == Command.ENVIRONMENT:
            return self._handle_command_environment(req, conn)

        elif command == Command.STATUS:
            return self._handle_command_status(req)

        elif command == Command.RESET:
            return self._handle_command_reset(req)

        elif command == Command.FEED:
            return self._handle_command_feed(req)

        elif command == Command.STREAM_OUTPUT:
            return self._handle_command_stream_output(req, conn)

        elif command == Command.SLEEP:
            return self._handle_command_sleep(req)

        elif command == Command.WAKEUP:
            return self._handle_command_wakeup(req)
        
        elif command == Command.WAIT_IDLE:
            return self._handle_command_wait_idle(req)

        elif command == Command.WAIT_EMPTY:
            return self._handle_command_wait_empty(req)

        elif command == Command.WAIT_ASLEEP:
            return self._handle_command_wait_asleep(req)

        else:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "unknown command"
            }





    def _handle_command_setup(self, req):
        if self.pipeline is not None and not self.pipeline.is_reset():
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "previous pipeline is still running"
            }

        taskchain = [(t["function"], t.get("args", set()), t["is_generator"]) for t in req["tasks"]]
        try:
            self.pipeline = pipeline.Pipeline(self.log_addr, name=req["name"], version=req["version"], taskchain=taskchain, benchmark_folder=self.benchmark_folder)
        except AssertionError as e:
            self.pipeline = None
            return {
                "node": req["node"],
                "command": Command.ERROR, 
                "describtion": traceback.format_exc()
            }

        n_workers = req["n_workers"]
        self.pipeline.boot(n_workers=n_workers if n_workers is not None else mp.cpu_count())
        
        mpl.log("pipeline setup: " + self.pipeline.get_name() + " v" + str(self.pipeline.get_version()))
        return {
            "node": req["node"],
            "command": Command.SETUP
        }

    def _handle_command_environment(self, req, conn):
        WORKING_DIR = os.path.expanduser("~/.pipeline-cluster")

        if not os.path.isdir(WORKING_DIR):
            os.makedirs(WORKING_DIR)

        for package in req["local"]:
            util.dict_to_dir(WORKING_DIR, package)
            package_path = os.path.join(WORKING_DIR, list(package)[0])
            package_name = os.path.basename(package_path)
            mpl.log("install local package: " + package_name, addr=self.log_addr)
            subprocess.call(["pip", "install", package_path], shell=False)

        for package_name in req["remote"]:
            subprocess.call(["pip", "install", package_name], shell=False)
            mpl.log("install remote package: " + package_name, addr=self.log_addr)


        mpl.log("finished installing packages, restart server")
        conn.send({
            "node": req["node"],
            "command": Command.ENVIRONMENT
        })
        os.execv(sys.executable, [sys.executable] + sys.argv)


    def _handle_command_status(self, req):
        assert self.start_time is not None

        if self.pipeline is None:
            return {
                "node": req["node"],
                "command": Command.STATUS,
                "start_time": self.start_time,
                "running": False
            }

        assert self.pipeline.is_running(), "The pipeline should run at this point because calling setup is also booting immediatly"

        return {
            "node": req["node"],
            "command": Command.STATUS,
            "name": self.pipeline.get_name(),
            "version": self.pipeline.get_version(),
            "n_cores": mp.cpu_count(),
            "awake": self.pipeline.is_awake(),
            "running": True,
            "start_time": self.start_time,
            "n_worker": self.pipeline.get_n_worker(),
            "n_idle": self.pipeline.get_n_idle()
        }

    def _handle_command_reset(self, req):
        if self.pipeline is not None:
            self.pipeline.reset()
            self.pipeline = None
            mpl.log("pipeline reset", self.log_addr)

        return {
            "node": req["node"],
            "command": Command.RESET
        }

    def _handle_command_feed(self, req):
        if self.pipeline is None or self.pipeline.is_reset():
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "The pipeline can only be feed when its running"
            }

        try:
            self.pipeline.feed(req["items"])
        except AssertionError as e:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": traceback.format_exc()
            }

        return {
            "node": req["node"],
            "command": Command.FEED
        }

    def _handle_command_stream_output(self, req, conn):
        if self.pipeline is None:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "The pipeline is currently not running"
            }

        while self.pipeline.wait_output(abort_on_sleep=False):
            output = self.pipeline.get_output()
            if output:
                conn.send({
                    "node": req["node"],
                    "command": Command.STREAM_OUTPUT,
                    "output": output
                })

        return {
            "node": req["node"],
            "command": Command.STREAM_END
        }

    def _handle_command_sleep(self, req):
        if self.pipeline is None:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "The pipeline is currently not running"
            }

        self.pipeline.sleep()
        return {
            "node": req["node"],
            "command": Command.SLEEP
        }
    
    def _handle_command_wakeup(self, req):
        if self.pipeline is None:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "The pipeline is currently not running"
            }

        self.pipeline.wakeup()
        return {
            "node": req["node"],
            "command": Command.WAKEUP
        }

    def _handle_command_wait_idle(self, req):
        if self.pipeline is None:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "The pipeline is currently not running"
            }

        ret = self.pipeline.wait_idle(abort_on_sleep=False)
        return {
            "node": req["node"],
            "command": Command.WAIT_IDLE,
            "reset": not ret,
            "n_idle": self.pipeline.get_n_idle()
        }

    def _handle_command_wait_empty(self, req):
        if self.pipeline is None:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "The pipeline is currently not running"
            }

        ret = self.pipeline.wait_empty()
        return {
            "node": req["node"],
            "command": Command.WAIT_EMPTY,
            "sleep": not ret
        }

    def _handle_command_wait_alseep(self, req):
        if self.pipeline is None:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "The pipeline is currently not running"
            }

        self.pipeline.wait_asleep()
        return {
            "node": req["node"],
            "command": Command.WAIT_ALSEEP
        }




class Client:
    def __init__(self, addr):
        self.addr = addr


    def send_command_setup(self, name, version, tasks, n_workers=None, local_packages=[], remote_packages=[], timeout=5, retry_sleep=1, restart_timeout=5):
        
        # environment setup
        status = self.send_command_status(retry=True, timeout=timeout, retry_sleep=retry_sleep)
        start_time = status["start_time"]

        conn = util.connect_timeout(self.addr, retry=True, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.ENVIRONMENT,
            "local": local_packages,
            "remote": remote_packages
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)
        
        # wait for node to restart
        wait_time = time.time()
        while True:
            if time.time() - wait_time > restart_timeout:
                raise TimeoutError("Node server not running again after restart")
            try:
                new_status = self.send_command_status(retry=True, timeout=timeout, retry_sleep=retry_sleep)
            except ConnectionResetError:
                continue
            
            if new_status["start_time"] != start_time:
                break
        
        # pipeline setup
        conn = util.connect_timeout(self.addr, retry=True, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.SETUP,
            "name": name,
            "version": version,
            "tasks": tasks,
            "n_workers": n_workers
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)



    def send_command_status(self, retry=True, timeout=5, retry_sleep=1):
        conn = util.connect_timeout(self.addr, retry=retry, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.STATUS
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)

        resp.pop("command", None)
        resp.pop("node", None)
        return resp

    
    def send_command_reset(self, retry=True, timeout=5, retry_sleep=1):
        conn = util.connect_timeout(self.addr, retry=retry, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.RESET
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)


    def send_command_sleep(self, retry=True, timeout=5, retry_sleep=1):
        conn = util.connect_timeout(self.addr, retry=retry, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.SLEEP
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)


    def send_command_wakeup(self, retry=True, timeout=5, retry_sleep=1):
        conn = util.connect_timeout(self.addr, retry=retry, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.WAKEUP
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)


    def send_command_feed(self, items, retry=True, timeout=5, retry_sleep=1):
        conn = util.connect_timeout(self.addr, retry=retry, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.FEED,
            "items": list(items)
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)


    def _stream_routine(self, output_handler, retry, timeout, retry_sleep):
        conn = util.connect_timeout(self.addr, retry=retry, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.STREAM_OUTPUT
        })

        while True:
            try:
                resp = conn.recv()
            except EOFError:
                break

            self._raise_if_error(resp)

            if resp["command"] == Command.STREAM_END:
                break

            if resp["output"]:
                threading.Thread(target=self._output_routine, args=(resp["output"], output_handler)).start()

        conn.close()  


    def _output_routine(self, items, output_handler):
        for item in items:
            output_handler(item)
        

    def send_command_stream_output(self, output_handler, detach=True, retry=True, timeout=5, retry_sleep=1):
        if detach:
            threading.Thread(target=self._stream_routine, args=(output_handler, retry, timeout, retry_sleep), daemon=True).start()
        else:
            self._stream_routine(output_handler, retry, timeout, retry_sleep)


    def send_command_wait_idle(self, retry=True, timeout=5, retry_sleep=1):
        conn = util.connect_timeout(self.addr, retry=retry, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.WAIT_IDLE
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)
        return (resp["reset"], resp["n_idle"])

    def send_command_wait_empty(self, retry=True, timeout=5, retry_sleep=1):
        conn = util.connect_timeout(self.addr, retry=retry, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.WAIT_EMPTY
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)
        return resp["sleep"]

    def send_command_wait_asleep(self, retry=True, timeout=5, retry_sleep=1):
        conn = util.connect_timeout(self.addr, retry=retry, retry_timeout=timeout, retry_sleep=retry_sleep)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.WAIT_ASLEEP
        })
        resp = conn.recv()
        conn.close()
        self._raise_if_error(resp)


    def _raise_if_error(self, resp):
        if resp["command"] == Command.ERROR:
            raise RuntimeError(resp["describtion"])

        if resp["command"] == Command.INTERNAL_ERROR:
            raise ConnectionResetError(resp["describtion"])