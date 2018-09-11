#! /usr/bin/python3

import socketserver
import sys
import time
from datetime import datetime

# Port number for server listening.
if len(sys.argv) > 2:
    listening_port = int(sys.argv[2])
else:
    listening_port = 2345


edge_path = sys.argv[1]
links = dict()




# edge_path is expected to be a file with the format SRC_ID	DST_ID	TIMESTAMP:
#1	20	1217964960
#1	24	1227241074
#1	25	1229314692
#38	39	1160092062
# Each timestamp for the Facebook New Orleans dataset has a granularity of 1 second.




with open(edge_path) as edge_file:
    for l in edge_file:
        if not l.startswith("#"):
            vv = l.strip().split("\t")
		# https://www.programiz.com/python-programming/methods/dictionary/setdefault
		# dict_obj.setdefault(a, b): 
		#	if a is present in dict_obj, return dict_obj[a]
		#	else set dict_objs[a] = b, return b
            links.setdefault(int(vv[2]), list()).append((vv[0], vv[1]))

start_date = min(links.keys())
end_date = max(links.keys())


class FacebookLinksStreamHandler(socketserver.BaseRequestHandler):
    queries = 0

    def handle(self):
        print("Accepted client at " + str(self.client_address))

        i = 0
		# A sequence of values starting at start_date all the day to end_date (inclusive)
        for instant in range(start_date, end_date + 1):
            if instant in links:
                for edge in links[instant]:
                    msg = "A {0} {1}\n".format(edge[0], edge[1])
                    self.request.send(msg.encode())
			
            i += 1
			# Each instant corresponds to one second in the context of the analyzed Facebook dataset.
			# Every 604800 dataset seconds, perform a query. This query corresponds to an interval 
			# of 7 days, amounting to a total of 125 queries.
            if i >= 604800:
                self.query(instant)
				# After each query, pause for 20 seconds before resuming edge addition.
                time.sleep(20)
                i = 0

        self.query(end_date)
        self.request.send("END".encode())

        print("Closed client connection.")

    def query(self, instant):
        self.queries += 1
        date_str = datetime.fromtimestamp(instant).strftime("%Y-%m-%d")
        msg = "Q " + date_str + "\n"
        print("Query", str(self.queries), ":", msg, end='')
        self.request.send(msg.encode())


server = socketserver.ThreadingTCPServer(("localhost", listening_port), FacebookLinksStreamHandler)
print("Listening...")
server.serve_forever()
