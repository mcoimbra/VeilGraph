#!/usr/bin/env python3
__copyright__ = """ Copyright 2018 Miguel E. Coimbra

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. """
__license__ = "Apache 2.0"

###########################################################################
################################# IMPORTS #################################
###########################################################################

# PEP 8 Style Guide for imports: https://www.python.org/dev/peps/pep-0008/#imports
# 1. standard library imports
import argparse
import datetime
import io
import os
import signal
import socketserver
import sys
import time
import threading
from typing import Tuple, List

# 2. related third party imports
# 3. custom local imports
from graphbolt import localutil

###########################################################################
############################## SERVER CLASS ###############################
###########################################################################

#TODO: catch exception "BrokenPipeError: [Errno 32] Broken pipe" and print appropriate message about the GraphBolt client having crashed or closed with Ctrl+C.

class DatasetStreamHandler(socketserver.BaseRequestHandler):
    queries = 0

    def handle(self):
        print("[CLIENT - {0}]\t\t{1}\n".format(str(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"), str(self.client_address)), file=self.server.out)

        deletions_size = int(self.server.chunk_sizes[0] * 0.2)

        i = 0
        
        deletion_index = 0

        for line in self.server.edge_lines:

            i = i + 1
            
            edge = line.strip().split('\t')

            msg = "A {0} {1}\n".format(edge[0], edge[1])
            self.request.send(msg.encode())
            time.sleep(0.017)

            if i == self.server.chunk_sizes[self.queries]:
                i = 0
                print("[QUERY\t- {0}]\t\t{1} - #{2}/{3} ({4})".format(str(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"), str(self.client_address), str(self.queries + 1), str(self.server.query_count), str(self.server.chunk_sizes[self.queries])), file=self.server.out)

                # Get current server.deletion_lines block.
                deletion_base = deletion_index * deletions_size
                target_deletion_lines = self.server.deletion_lines[deletion_base: deletion_base + deletions_size]
                deletion_index = deletion_index + 1

                # Send block's line with self.request.send
                delete_msg = ''
                for tmp in target_deletion_lines:
                    delete_edge = tmp.strip().split('\t')
                    delete_msg = delete_msg + "D {0} {1}\n".format(delete_edge[0], delete_edge[1])
                    
                self.request.send(delete_msg.encode())
                time.sleep(0.017)
                
                self.query()

                if not self.queries == self.server.query_count:
                    print("[INFO]\t\t\t{0} - Next chunk size: {1}".format(str(self.client_address), str(self.server.chunk_sizes[self.queries])), file=self.server.out)
                    self.server.out.flush()
                    time.sleep(20)


        print("[CLOSED\t- {0}]\tFinished streaming for client {1} ({2} queries sent)\n".format(str(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"), str(self.client_address), str(self.queries)), file=self.server.out)
        self.request.send("END".encode())

    def query(self):
        self.queries += 1
        msg = "Q {0}\n".format(str(self.queries))
        self.request.send(msg.encode())



def make_server(edge_lines: List, chunk_sizes: List, query_count: int, deletion_lines: List = [], listening_host: str = "localhost", listening_port: int = 2345, out: io.TextIOWrapper = sys.stdout) -> socketserver.ThreadingTCPServer:

    server = socketserver.ThreadingTCPServer((listening_host, listening_port), DatasetStreamHandler)
    server.edge_lines = edge_lines
    server.chunk_sizes = chunk_sizes
    server.query_count = query_count
    server.out = out
    server.deletion_lines = deletion_lines

    return server





if __name__ == "__main__":

    ###########################################################################
    ############################# READ ARGUMENTS ##############################
    ###########################################################################

    # The class argparse.RawTextHelpFormatter is used to keep new lines in help text.
    DESCRIPTION_TEXT = "GraphBolt streamer script."
    parser = argparse.ArgumentParser(description=DESCRIPTION_TEXT, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-i", "--input-file", help="dataset stream path.", required=True, type=str)
    parser.add_argument("-d", "--deletions-file", help="dataset deletions profile.", required=False, type=str, default='')
    parser.add_argument("-q", "--query-count", help="stream query count.", required=True, type=int)
    parser.add_argument("-p", "--port", help="desired listening stream port.", required=False, type=int, default=2345)
    parser.add_argument("--host", help="desired listening stream host.", required=False, type=str, default="localhost")

    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print("> Provided input file '--input-file' '{}' does not exist. Exiting.".format(args.input_file))
        sys.exit(1)
    
    if len(args.deletions_file) > 0 and not os.path.exists(args.deletions_file):
        print("> Provided deletions file '--deletions-file' '{}' does not exist. Exiting.".format(args.deletions_file))
        sys.exit(1)
    
    if args.query_count < 0:
        print("> Query count '--query-count' must be positive. Exiting.")
        sys.exit(1)
    if args.port < 0:
        print("> Stream port '--port' must be positive. Exiting.")
        sys.exit(1)

    chunk_size, chunk_sizes, edge_lines, edge_count, deletion_lines = localutil.prepare_stream(args.input_file, args.query_count, args.deletions_file)

    print('[INFO]')
    print('> Input file has {0} lines'.format(edge_count))
    print('query_count:\t{}'.format(args.query_count))
    print('line_count:\t{}'.format(edge_count))
    print('chunk_size:\t{}'.format(chunk_size))
    print('last_chunk_size:\t{}'.format(chunk_sizes[-1]))
    print('sum_count:\t{}'.format(sum(chunk_sizes)))

    server = make_server(edge_lines=edge_lines, chunk_sizes=chunk_sizes, query_count=args.query_count, deletion_lines=deletion_lines, listening_host=args.host, listening_port=args.port)
    


    ###########################################################################
    ############################ SIGNAL HANDLERS ##############################
    ###########################################################################
    

    t = threading.Thread(target=server.serve_forever)

    # Prepare the handler for SIGINT (Ctrl+C).
    def signal_handler(signal, frame):
        print('> Stopping execution...')
        server.shutdown()
        print('> Done.')
        sys.exit(0)

    # Assign the signal handlers.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    ###########################################################################
    ############################## LAUNCH SERVER ##############################
    ###########################################################################
    print("> Listening for clients...")
    t.start()

    DEFAULT_TIMEOUT = 5
    

    while t.isAlive: 
        t.join(DEFAULT_TIMEOUT)
