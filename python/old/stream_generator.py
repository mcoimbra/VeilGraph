import socketserver
import sys

# This file starts a ThreadingTCPServer listening at port sys.argv[3] with file name sys.argv[1].

class StreamRequestHandler(socketserver.BaseRequestHandler):
    file_name = ''
    interval = 1

    def handle(self):
        print("Accepted client at " + str(self.client_address))
        with open(self.file_name) as file:
            for line in file:
                self.request.send(line.encode())
                # time.sleep(self.interval / 10)

        print("End of client connection.")


def main():
    if len(sys.argv) < 3:
        print("Usage: stream_generator.py <file name> <stream interval> <port>")
        sys.exit(-1)

    StreamRequestHandler.file_name = sys.argv[1]
    StreamRequestHandler.interval = int(sys.argv[2])
    port = int(sys.argv[3])

    server = socketserver.ThreadingTCPServer(("localhost", port), StreamRequestHandler)
    print("Listening...")
    server.serve_forever()


if __name__ == "__main__":
    main()
