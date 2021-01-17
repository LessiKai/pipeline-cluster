
import threading
import multiprocessing as mp
import multiprocessing.connection as mpc
from util import connect_timeout
from pipeline import Pipeline

class PipelineNodeCommand:
    ERROR = "ERROR"
    SETUP = "SETUP"
    STATUS = "STATUS"
    BOOT = "BOOT"
    RESET = "RESET"
    WAKEUP = "WAKEUP"
    SLEEP = "SLEEP"
    FEED = "FEED"
    STREAM_OUTPUT = "STREAM_OUTPUT"
    STREAM_END = "STREAM_END"
    WAIT_IDLE = "WAIT_IDLE"
    WAIT_EMPTY = "WAIT_EMPTY"
    WAIT_ASLEEP = "WAIT_ASLEEP"




class PipelineClusterNode:
    def __init__(self, addr, log_addr, conn_buffer_size=2):
        self.addr = addr
        self.conn_buffer_size = conn_buffer_size
        self.log_addr = log_addr
        self.pipeline = None

    def serve(self):
        with mpc.Listener(self.addr, "AF_INET", self.conn_buffer_size, None) as lst:
            while True:
                conn = lst.accept()
                threading.Thread(target=self._handle_connection, args=(conn, lst.last_accepted)).start()

    def _handle_connection(self, conn, caddr):
        while True:
            try:
                req = conn.recv()
                response = self._handle_request(conn, req)
                conn.send(response)
            except EOFError as e: # maybe this should catch all exceptions in case the client disconnects while sending
                break
        
        conn.close()

    def _handle_request(self, conn, req):
        if type(req) != dict:
            return {
                "command": PipelineNodeCommand.ERROR,
                "describtion": "request has to be of type dict"
            }

        command = req["command"]
        
        if command == PipelineNodeCommand.SETUP:

            if self.pipeline is not None and not self.pipeline.is_reset():
                return {
                    "node": req["node"],
                    "command": PipelineNodeCommand.ERROR,
                    "describtion": "previous pipeline is still running"
                }
            self.pipeline = Pipeline(self.log_addr, name=req["name"], version=req["version"]) # TODO:add a lock or just dont call it while another process is requesting 
            for task in req["tasks"]:
                try:
                    self.pipeline.add_task(task["function"], is_generator=task["is_generator"])
                except:
                    pass # TODO: currently an assertion thus no exception, maybe change? (pipeline add_task)

            return {
                "node": req["node"],
                "command": command
            }

        elif command == PipelineNodeCommand.STATUS:
            return {
                "node": req["node"],
                "command": command,
                "awake": self.pipeline.is_awake(),
                "running": self.pipeline.is_running(),
                "n_worker": self.pipeline.get_n_worker(),
                "n_idle": self.pipeline.get_n_idle()
            }

        elif command == PipelineNodeCommand.BOOT:
            n_worker = req["n_worker"] if req["n_worker"] is not None else mp.cpu_count()
            try:
                self.pipeline.boot(n_worker=n_worker)
            except Exception as e:
                return {
                    "node": req["node"],
                    "command": PipelineNodeCommand.ERROR,
                    "describtion": str(e)
                }
            return {
                "node": req["node"],
                "command": command
            }

        elif command == PipelineNodeCommand.RESET:
            try:
                self.pipeline.reset()
            except Exception as e:
                return {
                    "node": req["node"],
                    "command": PipelineNodeCommand.ERROR,
                    "describtion": str(e)
                }
            return {
                "node": req["node"],
                "command": command
            }

        elif command == PipelineNodeCommand.FEED:
            try:
                self.pipeline.feed(*req["items"])
            except Exception as e:
                return {
                    "node": req["node"],
                    "command": PipelineNodeCommand.ERROR,
                    "descibtion": str(e)
                }
            return {
                "node": req["node"],
                "command": command
            }

        elif command == PipelineNodeCommand.STREAM_OUTPUT:

            while self.pipeline.wait_output():
                output = self.pipeline.get_output()
                if output:
                    conn.send({
                        "node": req["node"],
                        "command": command,
                        "output": output
                    })

            return {
                "node": req["node"],
                "command": PipelineNodeCommand.STREAM_END
            }

        elif command == PipelineNodeCommand.SLEEP:
            self.pipeline.sleep()
            return {
                "node": req["node"],
                "command": command
            }

        elif command == PipelineNodeCommand.WAKEUP:
            self.pipeline.wakeup()
            return {
                "node": req["node"],
                "command": command
            }
        
        elif command == PipelineNodeCommand.WAIT_IDLE:
            ret = self.pipeline.wait_idle()
            return {
                "node": req["node"],
                "command": command,
                "sleep": not ret,
                "n_idle": self.pipeline.get_n_idle()
            }

        elif command == PipelineNodeCommand.WAIT_EMPTY:
            ret = self.pipeline.wait_empty()
            return {
                "node": req["node"],
                "command": command,
                "sleep": not ret
            }

        elif command == PipelineNodeCommand.WAIT_ASLEEP:
            self.pipeline.wait_asleep()
            return {
                "node": req["node"],
                "command": command
            }

        else:
            return {
                "node": req["node"],
                "command": PipelineNodeCommand.ERROR,
                "describtion": "command not known"
            }




class PipelineClusterNodeClient:
    def __init__(self, addr):
        self.addr = addr


    def send_command_setup(self, name, version, tasks):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.SETUP,
            "name": name,
            "version": version,
            "tasks": tasks
        })
        resp = conn.recv()
        conn.close()
        if resp["command"] == PipelineNodeCommand.ERROR:
            raise Exception(resp["describtion"])


    def send_command_status(self):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.STATUS
        })
        resp = conn.recv()
        conn.close()
        resp.pop("command", None)
        return resp
    
    def send_command_boot(self, n_worker=None):
        """
        n_worker = some int
        n_worker = None -> mp.cpu_count()
        """
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.BOOT,
            "n_worker": n_worker
        })
        resp = conn.recv()
        conn.close()
        if resp["command"] == PipelineNodeCommand.ERROR:
            raise Exception(resp["describtion"])

    
    def send_command_reset(self):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.RESET
        })
        resp = conn.recv()
        conn.close()
        if resp["command"] == PipelineNodeCommand.ERROR:
            raise Exception(resp["describtion"])

    def send_command_sleep(self):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.SLEEP
        })
        resp = conn.recv()
        conn.close()

    def send_command_wakeup(self):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.WAKEUP
        })
        resp = conn.recv()
        conn.close()


    def send_command_feed(self, *items):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.FEED,
            "items": list(items)
        })
        resp = conn.recv()
        conn.close()

        if resp["command"] == PipelineNodeCommand.ERROR:
            raise Exception(resp["describtion"])


    def _stream_routine(self, output_handler):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.STREAM_OUTPUT
        })

        while True:
            try:
                response = conn.recv()
            except EOFError:
                break

            if response["command"] == PipelineNodeCommand.STREAM_END:
                break

            if response["output"]:
                threading.Thread(target=output_handler, args=(response["output"],)).start()
        
        conn.close()


    def send_command_stream_output(self, output_handler, detach=True):
        if detach:
            threading.Thread(target=self._stream_routine, args=(output_handler, )).start()
        else:
            self._stream_routine(output_handler)

    def send_command_wait_idle(self):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.WAIT_IDLE
        })
        resp = conn.recv()
        conn.close()
        return (resp["sleep"], resp["n_idle"])

    def send_command_wait_empty(self):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.WAIT_EMPTY
        })
        resp = conn.recv()
        conn.close()
        return resp["sleep"]

    def send_command_wait_asleep(self):
        conn = connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": PipelineNodeCommand.WAIT_ASLEEP
        })
        resp = conn.recv()
        conn.close()

