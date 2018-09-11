#! /usr/bin/python3

import socketserver
import sys
import time
from datetime import datetime

edge_path = sys.argv[1]
dates_path = sys.argv[2]

citations = {}
publication_dates = {}

with open(edge_path) as edge_file:
    for l in edge_file:
        if not l.startswith("#"):
            vv = l.strip().split("\t")
            citations.setdefault(int(vv[0]), list()).append(int(vv[1]))

with open(dates_path) as dates_file:
    for l in dates_file:
        if not l.startswith("#"):
            dd = l.strip().split("\t")
            publication_dates.setdefault(datetime.strptime(dd[1], "%Y-%m-%d").toordinal(), list()).append(int(dd[0]))

start_date = min(publication_dates.keys())
end_date = max(publication_dates.keys())


class CitationStreamHandler(socketserver.BaseRequestHandler):
    queries = 0

    def handle(self):
        print("Accepted client at " + str(self.client_address))

        i = 0
        for day in range(start_date, end_date + 1):
            for paper in publication_dates.get(day, list()):
                for c in citations.get(paper, list()):
                    msg = "A {0} {1}\n".format(paper, c)
                    self.request.send(msg.encode())
            i += 1
            if i % 61 == 0:
                self.query(day)

            time.sleep(0.01)

        self.query(end_date)
        self.request.send("END".encode())

        print("Closing client connection...")

    def query(self, day):
        self.queries += 1
        date_str = datetime.fromordinal(day).strftime("%Y-%m-%d")
        msg = "Q " + date_str + "\n"
        print("Query", str(self.queries), ":", msg, end='')
        self.request.send(msg.encode())


server = socketserver.ThreadingTCPServer(("localhost", 1234), CitationStreamHandler)
print("Listening...")
server.serve_forever()
