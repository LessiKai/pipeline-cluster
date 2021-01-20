#!/usr/bin/env python3
import pipeline_cluster.multiprocess_logging as mpl
import argparse
import os
import signal

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda signum, frame: exit(0))
    signal.signal(signal.SIGTERM, lambda signum, frame: exit(0))

    parser = argparse.ArgumentParser(description="log server CLI of the pipeline-cluster package")
    parser.add_argument("--interface", "-i", type=str, default="localhost")
    parser.add_argument("--port", "-p", type=int, default=5555)
    parser.add_argument("--file", "-f", type=str, default="pipeline-cluster-log.txt")
    parser.add_argument("--buffer", "-b", type=int, default=2)

    args = parser.parse_args()

    logfile = os.path.abspath(args.file)
    if os.path.isfile(args.file):
        append = (input(logfile + " already exists. Proceed? (y/n) ").strip() in ["y", "Y", "j", "J"])
        if not append:
            exit(1)
        
    if args.buffer < 1:
        print("The connection buffer size has to be greater than 0")
        exit(1)

    port_range = (0, 65535)
    if args.port < port_range[0] or args.port > port_range[1]:
        print("Port " + str(args.port) + " is not in the valid port range of " + str(port_range))

    # TODO: checks for ip address

    addr = (args.interface, args.port)
    print("serving at " + args.interface + ":" + str(args.port))
    print("logfile: " + logfile)
    print("connection buffer size: " + str(args.buffer))
    mpl.serve(addr, logfile, args.buffer, detach=False)