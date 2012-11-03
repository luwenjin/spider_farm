#coding:utf-8
import logging
import base64
import json
from snakemq.link import Link
from snakemq.message import Message
from snakemq.messaging import Messaging
from snakemq.packeter import Packeter
import zlib

#=================================================== setup logger
logging.basicConfig(filename="spider_farm.log", level=logging.DEBUG)

class SnakeFilter(logging.Filter):
    def filter(self, record):
        return not record.name.startswith('snakemq')

formatter = logging.Formatter('%(name)-8s %(levelname)-8s: %(message)s')

console = logging.StreamHandler()
console.setFormatter(formatter)
console.setLevel(logging.DEBUG)
console.addFilter(SnakeFilter())

logging.getLogger().addHandler(console)

log = logging.getLogger('base')
#=====================================================

SRC_MID64 = 'src_mid64'

class MessageQueue(object):
    def __init__(self, my_ident):
        self.me = my_ident
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

    def on_receive_json(self, ident, obj, mid64):
        pass

    def send_json(self, ident, obj, ttl=600):
        _json = json.dumps(obj)
        message = Message(bytes(_json), ttl)
        log.debug('send_json -> [%s]: %s %s', ident, obj, repr(get_mid64(message)))
        self.messaging.send_message(ident, message)
        return message

    def on_connect(self, conn, ident):
        pass

    def on_recv(self, conn, ident, message):
        log.debug('on_recv <- [%s]: %s', ident, message)
        mid64 = get_mid64(message)
        try:
            obj = json.loads(message.data)
            if not obj.has_key('src_mid64'):
                obj['src_mid64'] = mid64
            self.on_receive_json(ident, obj, mid64)
        except ValueError:
            self.send_json(ident, {'error': 'invalid_json', 'message': str(message.data), 'src_mid64': mid64})
            log.error('invalid_json: %s', message.data)


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


def get_mid64(message):
    return base64.encodestring(message.uuid)


if __name__ == '__main__':
    b = to_zip64(u'hello你好')
    print from_zip64(b)