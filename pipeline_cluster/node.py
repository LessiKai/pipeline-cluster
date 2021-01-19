import threading
import multiprocessing as mp
import multiprocessing.connection as mpc
from pipeline_cluster import util
from pipeline_cluster import pipeline

class Command:
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




class Server:
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
                "command": Command.ERROR,
                "describtion": "request has to be of type dict"
            }

        command = req["command"]
        
        if command == Command.SETUP:

            if self.pipeline is not None and not self.pipeline.is_reset():
                return {
                    "node": req["node"],
                    "command": Command.ERROR,
                    "describtion": "previous pipeline is still running"
                }
            self.pipeline = pipeline.Pipeline(self.log_addr, name=req["name"], version=req["version"]) # TODO:add a lock or just dont call it while another process is requesting 
            for task in req["tasks"]:
                try:
                    self.pipeline.add_task(task["function"], is_generator=task["is_generator"])
                except:
                    pass # TODO: currently an assertion thus no exception, maybe change? (pipeline add_task)

            return {
                "node": req["node"],
                "command": command
            }

        elif command == Command.STATUS:
            return {
                "node": req["node"],
                "command": command,
                "awake": self.pipeline.is_awake(),
                "running": self.pipeline.is_running(),
                "n_worker": self.pipeline.get_n_worker(),
                "n_idle": self.pipeline.get_n_idle()
            }

        elif command == Command.BOOT:
            n_worker = req["n_worker"] if req["n_worker"] is not None else mp.cpu_count()
            try:
                self.pipeline.boot(n_worker=n_worker)
            except Exception as e:
                return {
                    "node": req["node"],
                    "command": Command.ERROR,
                    "describtion": str(e)
                }
            return {
                "node": req["node"],
                "command": command
            }

        elif command == Command.RESET:
            try:
                self.pipeline.reset()
            except Exception as e:
                return {
                    "node": req["node"],
                    "command": Command.ERROR,
                    "describtion": str(e)
                }
            return {
                "node": req["node"],
                "command": command
            }

        elif command == Command.FEED:
            try:
                self.pipeline.feed(*req["items"])
            except Exception as e:
                return {
                    "node": req["node"],
                    "command": Command.ERROR,
                    "descibtion": str(e)
                }
            return {
                "node": req["node"],
                "command": command
            }

        elif command == Command.STREAM_OUTPUT:

            while self.pipeline.wait_output(abort_on_sleep=False):
                output = self.pipeline.get_output()
                if output:
                    conn.send({
                        "node": req["node"],
                        "command": command,
                        "output": output
                    })

            return {
                "node": req["node"],
                "command": Command.STREAM_END
            }

        elif command == Command.SLEEP:
            self.pipeline.sleep()
            return {
                "node": req["node"],
                "command": command
            }

        elif command == Command.WAKEUP:
            self.pipeline.wakeup()
            return {
                "node": req["node"],
                "command": command
            }
        
        elif command == Command.WAIT_IDLE:
            ret = self.pipeline.wait_idle(abort_on_sleep=False)
            return {
                "node": req["node"],
                "command": command,
                "reset": not ret,
                "n_idle": self.pipeline.get_n_idle()
            }

        elif command == Command.WAIT_EMPTY:
            ret = self.pipeline.wait_empty()
            return {
                "node": req["node"],
                "command": command,
                "sleep": not ret
            }

        elif command == Command.WAIT_ASLEEP:
            self.pipeline.wait_asleep()
            return {
                "node": req["node"],
                "command": command
            }

        else:
            return {
                "node": req["node"],
                "command": Command.ERROR,
                "describtion": "command not known"
            }




class Client:
    def __init__(self, addr):
        self.addr = addr


    def send_command_setup(self, name, version, tasks):
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.SETUP,
            "name": name,
            "version": version,
            "tasks": tasks
        })
        resp = conn.recv()
        conn.close()
        if resp["command"] == Command.ERROR:
            raise Exception(resp["describtion"])


    def send_command_status(self):
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.STATUS
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
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.BOOT,
            "n_worker": n_worker
        })
        resp = conn.recv()
        conn.close()
        if resp["command"] == Command.ERROR:
            raise Exception(resp["describtion"])

    
    def send_command_reset(self):
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.RESET
        })
        resp = conn.recv()
        conn.close()
        if resp["command"] == Command.ERROR:
            raise Exception(resp["describtion"])

    def send_command_sleep(self):
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.SLEEP
        })
        resp = conn.recv()
        conn.close()

    def send_command_wakeup(self):
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.WAKEUP
        })
        resp = conn.recv()
        conn.close()


    def send_command_feed(self, *items):
        conn = util.connect_timeout(self.addr, retry=True)
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

        if resp["command"] == Command.ERROR:
            raise Exception(resp["describtion"])


    def _stream_routine(self, output_handler):
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.STREAM_OUTPUT
        })

        while True:
            try:
                response = conn.recv()
            except EOFError:
                break

            if response["command"] == Command.STREAM_END:
                break

            if response["output"]:
                threading.Thread(target=self._output_routine, args=(response["output"], output_handler)).start()
        
        conn.close()

    def _output_routine(self, items, output_handler):
        for item in items:
            output_handler(item)
        

    def send_command_stream_output(self, output_handler, detach=True):
        if detach:
            threading.Thread(target=self._stream_routine, args=(output_handler, )).start()
        else:
            self._stream_routine(output_handler)

    def send_command_wait_idle(self):
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.WAIT_IDLE
        })
        resp = conn.recv()
        conn.close()
        return (resp["reset"], resp["n_idle"])

    def send_command_wait_empty(self):
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.WAIT_EMPTY
        })
        resp = conn.recv()
        conn.close()
        return resp["sleep"]

    def send_command_wait_asleep(self):
        conn = util.connect_timeout(self.addr, retry=True)
        conn.send({
            "node": {
                "ip": self.addr[0],
                "port": self.addr[1]
            },
            "command": Command.WAIT_ASLEEP
        })
        resp = conn.recv()
        conn.close()

