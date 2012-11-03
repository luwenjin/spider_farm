#coding:utf-8
import thread
import time
from datetime import datetime

from base import MessageQueue, logging, SRC_MID64, get_mid64

log = logging.getLogger('server')


class SpiderServer(MessageQueue):
    def __init__(self, my_ident, port):
        super(SpiderServer, self).__init__(my_ident)
        self.listen(port)
        self.commands = {} # type, request, message, worker, sent_at
        self.contacts = {} # waiting_reply: True/False, role: worker/None

    def send_command(self, ident, _type, **kwargs):
        kwargs["type"] = _type
        msg = self.send_json(ident, kwargs)
        mid64 = get_mid64(msg)
        self.contacts[ident]['waiting_reply'] = True
        self.commands[mid64] = {
            'sent_at': datetime.now(),
            'type': kwargs.pop('type'),
            'request': kwargs,
            'message': msg,
            'worker': ident,
        }

    def workers(self):
        items = self.contacts.items()
        return filter(lambda x: x[1].get('role') == 'worker', items)

    def on_receive_json(self, worker, obj, mid64):
        log.debug('on_receive_json <- [%s]: %s %s', worker, repr(mid64), obj)
        if obj.has_key(SRC_MID64):
            # got reply
            cmd_mid64 = obj[SRC_MID64]
            if self.commands.has_key(cmd_mid64):
                request = self.commands.pop(cmd_mid64)
                self.process_reply(worker, obj, request)
            else:
                log.warning('reply not in commands <- [%s]: %s %s', worker, obj, mid64)
            self.contacts[worker]['waiting_reply'] = False
        else:
            # got request
            self.process_request(obj)

    def process_reply(self, worker, response, request):
        # 处理command的回复
        if response.get('status') == 'error':
            log.warning('cmd response error: [%s] %s', worker, response)
        if request.get('type') == 'who':
            self._process_reply_who(worker, response, request)

    def _process_reply_who(self, worker, response, request):
        self.contacts[worker]['role'] = response.get('role')

    def process_request(self, request):
        # 处理‘请求’
        pass

    def on_loop(self):
        # 循环检查command是否timeout
        timeout_ids = []
        for mid64 in self.commands:
            cmd = self.commands[mid64]
            dt = datetime.now() - cmd['sent_at']
            if dt.total_seconds() > cmd['message'].ttl:
                timeout_ids.append(mid64)
        for mid64 in timeout_ids:
            cmd = self.commands.pop(mid64)
            log.warning('cmd timeout: %s', cmd)

    def on_connect(self, conn, ident):
        self.contacts[ident] = {
            'waiting_reply': False,
        }
        self.send_command(ident, 'who')

    def on_disconnect(self, conn, ident):
        if self.contacts.has_key(ident):
            self.contacts.pop(ident)
            log.info('disconnect [%s] (worker)', ident)
        else:
            log.info('disconnect [%s]', ident)


if __name__ == '__main__':
    server = SpiderServer('server', 9528)
    thread.start_new_thread(server.loop, ())

    while 1:
        print 'sleep'
        time.sleep(10)