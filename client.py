#coding:utf-8
import thread
import time

from base import MessageQueue, logging

log = logging.getLogger('client')


class SpiderClient(MessageQueue):
    def send_request(self, cmd, **kwargs):
        kwargs["type"] = 'request'
        kwargs['cmd'] = cmd
        self.send_json('server', kwargs)

    def on_receive_json(self, ident, obj, mid64):
        log.debug('on_receive_json <- [%s]: %s %s', ident, repr(mid64), obj)


if __name__ == '__main__':
    client = SpiderClient('client')
    client.connect('getf5.com', 44444)

    thread.start_new_thread(client.loop, ())

    while 1:
        print 'sleep'
        time.sleep(10)