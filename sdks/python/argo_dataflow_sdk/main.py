from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import signal
from datetime import datetime
import sys, os, threading

hostName = "0.0.0.0"
serverPort = 8080
threads = set()

class MyServer(BaseHTTPRequestHandler):
  handler = None
  def do_GET(self):  # GET /ready
    threads.add(threading.currentThread().getName())
    self.send_response(204)
    self.end_headers()
    threads.remove(threading.currentThread().getName())

  def do_POST(self):  # POST /messages
    try:
      threads.add(threading.currentThread().getName())
      len = int(self.headers.get('Content-Length'))
      msg = self.rfile.read(len)
      out = self.handler(msg, {})
      if out:
        self.send_response(201)
        self.end_headers()
        self.wfile.write(out)
      else:
        self.send_response(204)
        self.end_headers()
    except Exception as err:
      exception_type = type(err).__name__
      print(exception_type)
      self.send_response(500)
      self.end_headers()
      self.wfile.write(err.__str__().encode('UTF-8'))
    finally:
      threads.remove(threading.currentThread().getName())

class ProcessHandler:
  webServer = None
  keepRunning = True

  def shouldKeepRunning(self):
    return self.keepRunning

  def terminate(self, signal, frame):
    print("Start Terminating: %s" % datetime.now(), signal)
    self.keepRunning = False
    while len(threads):
      continue

    self.webServer.server_close()
    sys.exit(0)

  def start(self, handler):
    signal.signal(signal.SIGTERM, self.terminate)
    MyServer.handler = handler
    self.webServer = ThreadingHTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s with pid %i" % (hostName, serverPort, os.getpid()))
    try:
      while self.shouldKeepRunning():
        self.webServer.handle_request()
    except KeyboardInterrupt:
      print("Start Terminating from KeyboardInterrupt: %s" % datetime.now())
      pass

    print('Shutting server down.')
    self.webServer.server_close()
    sys.exit(0)

def default_handler(myServer, message, empty):
  msg = message.decode("UTF-8")
  print('Got message', msg)
  return ("Hi " + msg).encode('UTF-8')

def default_handler_error(myServer, message, empty):
  msg = message.decode("UTF-8")
  print('Got message', msg)
  raise ValueError('Some error')

if __name__ == '__main__':
  processHandler = ProcessHandler()
  processHandler.start(default_handler)