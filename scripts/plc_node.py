#!/usr/bin/env python3
import pipeline_cluster.node
import argparse
import signal


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="node server CLI of the pipeline-cluster package")
    parser.add_argument("--interface", "-i", type=str, default="localhost")
    parser.add_argument("--port", "-p", type=int, default=5600)
    parser.add_argument("--buffer", "-b", type=int, default=2)
    parser.add_argument("--log-interface", "-li", type=str, default="localhost")
    parser.add_argument("--log-port", "-lp", type=int, default=5555)

    args = parser.parse_args()

    if args.buffer < 1:
        print("The connection buffer size has to be greater than 0")
        exit(1)

    port_range = (0, 65535)
    if args.port < port_range[0] or args.port > port_range[1]:
        print("Port " + str(args.port) + " is not in the valid port range of " + str(port_range))

    if args.log_port < port_range[0] or args.log_port > port_range[1]:
        print("Port " + str(args.log_port) + " is not in the valid port range of " + str(port_range))

    print("serving at " + args.interface + ":" + str(args.port))
    print("log server: " + args.log_interface + ":" + str(args.log_port))
    print("connection buffer size: " + str(args.buffer))
    pipeline_cluster.node.Server((args.interface, args.port), (args.log_interface, args.log_port), conn_buffer_size=args.buffer).serve()
