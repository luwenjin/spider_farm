#coding:utf-8
import base64
import json
from snakemq.link import Link
from snakemq.message import Message
from snakemq.messaging import Messaging
from snakemq.packeter import Packeter
import zlib

__author__ = 'Administrator'

class MessageQueue(object):
    def __init__(self, my_ident):
        self.link = Link()
        self.packeter = Packeter(self.link)
        self.messaging = Messaging(my_ident, '', self.packeter)

        self.messaging.on_message_recv.add(self.on_recv)
        self.messaging.on_disconnect.add(self.on_disconnect)
        self.messaging.on_connect.add(self.on_connect)

    def loop(self):
        self.link.loop()

    def listen(self, port=4000):
        self.link.add_listener(('', port))

    def connect(self, host, port):
        self.link.add_connector((host, port))

    def cleanup(self):
        self.link.cleanup()

    def send_message(self, ident, message, ttl=600):
        message = Message(bytes(message), ttl)
        self.messaging.send_message(ident, message)

    def send_json(self, ident, obj):
        self.send_message(ident, json.dumps(obj))

    def on_connect(self, conn, ident):
        pass

    def on_recv(self, conn, ident, message):
        pass

    def on_disconnect(self, conn, ident):
        pass

    def on_loop(self):
        pass


def to_zip(s):
    if type(s) == unicode:
        s = s.encode('utf-8')
    return zlib.compress(s)


def from_zip(bytes):
    return zlib.decompress(bytes)


def to_zip64(s):
    bytes = to_zip(s)
    b64 = base64.encodestring(bytes)
    return b64


def from_zip64(s):
    bytes = base64.decodestring(s)
    return from_zip(bytes)


if __name__ == '__main__':

    b = to_zip64(u'hello你好')
    print from_zip64(b)