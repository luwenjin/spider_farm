import thread
import time
from base import MessageQueue

class SpiderServer(MessageQueue):
    def __init__(self, my_ident, port):
        super(SpiderServer, self).__init__(my_ident)
        self.listen(port)

    def on_recv(self, conn, ident, message):
        print message.data

    def on_connect(self, conn, ident):
        self.send_message(ident, '%s connected to server' % ident)





if __name__ == '__main__':
    server = SpiderServer('server', 9527)
    thread.start_new_thread(server.loop, ())

    while 1:
        print 'sleep'
        time.sleep(10)