#!/usr/bin/python3
import socketserver
import time
import datetime
import os
import argparse

def file_len(fname):
    print(str(fname))
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

# Prepare helper text and argument parsing.
home_dir = default=os.path.expanduser("~") 
description_text = ""

# The class argparse.RawTextHelpFormatter is used to keep new lines in help text.
parser = argparse.ArgumentParser(description=description_text, formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-i', '--input-file', help='path to file with tab-separated edges', type=str, required=True)
parser.add_argument('-p', '--port', help='port number', type=int, required=False, default=2345)
parser.add_argument('-a', '--address', help='server address', type=str, required=False, default='localhost')
parser.add_argument('-cc', '--chunk_count', help='total chunk count sent to each client', type=int, required=False, default=-1)
parser.add_argument('-cs', '--chunk_size', help='size of each chunk sent to the client', type=int, required=False, default=-1)
parser.add_argument('-s', '--stop_after', help='stop serving a client after this ammount of chunks has been sent', type=int, required=False, default=-1)

args = parser.parse_args()
# edge_path is expected to be a file with the format SRC_ID	DST_ID
#1	20

if args.chunk_count < -1 or args.chunk_count == 0:
    print("Error: chunk_count can either be -1 or positive. Exiting.")
    exit(-1)

if args.chunk_size < -1 or args.chunk_size == 0:
    print("Error: chunk_size can either be -1 or positive. Exiting.")
    exit(-1)

if args.chunk_count == -1 and args.chunk_size == -1:
    print("Error: chunk_count and chunk_size cannot both be -1. Exiting.")
    exit(-1)

if args.stop_after < -1 or args.stop_after == 0:
    print("Error: stop_after must be -1 or positive. Exiting.")
    exit(-1)



edge_path = args.input_file
edge_file = open(edge_path)
edge_count = file_len(edge_path)

edge_lines = edge_file.readlines()
if edge_lines[0].startswith('#'):
    edge_count = edge_count - 1
if edge_lines[-1] == "\n":
    edge_count = edge_count - 1

print('Input file has {0} lines'.format(edge_count))



if args.chunk_count == -1: # only size was provided
    chunk_count = int(edge_count / args.chunk_size)
    chunk_size = args.chunk_size
    chunk_sizes = (chunk_count - 1) * [chunk_size]
    chunk_sizes.append(edge_count - sum(chunk_sizes))
elif args.chunk_size == -1: # only chunks was provided
    chunk_count = args.chunk_count
    chunk_size = int(edge_count / args.chunk_count)
    chunk_sizes = (chunk_count - 1) * [chunk_size]
    chunk_sizes.append(edge_count - sum(chunk_sizes))
else:
    chunk_count = args.chunk_count
    chunk_size = args.chunk_size
    chunk_sizes = (chunk_count) * [chunk_size]


print('[INFO]')
print('chunk_count:\t{0}'.format(str(chunk_count)))
print('line_count:\t{0}'.format(str(edge_count)))
print('chunk_size:\t{0}'.format(str(chunk_size)))
print('last_chunk_size:\t{0}'.format(str(chunk_sizes[-1])))
print('sum_count:\t{0}'.format(str(sum(chunk_sizes))))
print('number of windows:\t{0}'.format(str(len(chunk_sizes))))

class SimpleStreamHandler(socketserver.BaseRequestHandler):
    chunk_counter = 0

    #def query(self):
    #    self.request.send("Q".encode())

    def handle(self):
        print("[ACCEPTED - {0}]\t{1}".format(str(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"), str(self.client_address)))
        i = 0
        for line in edge_lines:
            if line.startswith("#"):
                continue
            i = i + 1

            #print('sending: {0}', line)
            self.request.send(('A ' + line).encode())

            time.sleep(0.017)
            if i == chunk_sizes[self.chunk_counter]:
                self.request.send("Q\n".encode())
                #self.query()
                i = 0 # reset to the element counter for the next chunk
                self.chunk_counter += 1
                if not self.chunk_counter == chunk_count: # if we haven't sent all chunks yet
                    print('[INFO]\t{0} - Next chunk size: {1}'.format(str(self.client_address), str(chunk_sizes[self.chunk_counter])))
                    if args.stop_after > 0:
                        args.stop_after = args.stop_after - 1
                    time.sleep(5)
                    if args.stop_after == 0: # if there was a user argument to only send X packages out of Y total.
                        break
                else: # if we have sent all elements, finish.
                    break

        self.request.send("\nEND".encode())
        #self.server.shutdown()
        #self.server.server_close()
        print('[CLOSED - {0}]\tFinished streaming for client {1} ({2} update blocks sent)'.format(str(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"), str(self.client_address), str(self.chunk_counter)))

server = socketserver.ThreadingTCPServer((args.address, args.port), SimpleStreamHandler)
print("Listening for Apache Flink GraphBolt clients...")
print('{0}:{1}'.format(args.address, str(args.port)))
server.serve_forever()
