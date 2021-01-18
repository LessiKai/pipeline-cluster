import socket
import threading
import multiprocessing as mp
import multiprocessing.connection as mpc
import logging
import sys
import signal
import time
from pipeline_cluster import util


def _handle_connection(conn, caddr):
    while True:
        try:
            msg = conn.recv()
            logging.debug(msg)
            conn.send("OK")
        except EOFError as e: # maybe this should catch all exceptions in case the client disconnects while sending
            break
    
    conn.close()
    

def _serve(addr, conn_buffer_size, filename):
    logging.basicConfig(filename=filename, filemode="a", encoding="utf-8", level=logging.DEBUG)
    root_logger = logging.getLogger()
    log_handler = logging.StreamHandler(sys.stderr)
    log_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(log_handler)
    
    with mpc.Listener(addr, "AF_INET", conn_buffer_size, None) as lst:
        while True:
            conn = lst.accept()
            caddr = lst.last_accepted
            conn_thread = threading.Thread(target=_handle_connection, args=(conn, caddr))
            conn_thread.start()
        

def serve(addr, filename, conn_buffer_size=2, detach=False):
    if detach:
        proc = mp.Process(target=_serve, args=(addr, conn_buffer_size, filename), daemon=True).start()
    else:
        _serve(addr, conn_buffer_size, filename)


server_address = ("", 5555)

def configure(log_addr):
    global server_address
    server_address = log_addr

def log(msg):
    conn = util.connect_timeout(server_address, retry=True)
    conn.send(msg)
    response = conn.recv()
    if response != "OK":
        raise ConnectionError()


