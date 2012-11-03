#coding:utf-8
from copy import deepcopy
import random
import time
from datetime import datetime

from base import MessageQueue, logging

log = logging.getLogger('server')

class User(object):
    def __init__(self, name):
        self.name = name
        self.role = 'user'
        self.waiting_reply = False

    def waiting(self, flag=None):
        if flag is None:
            return self.waiting_reply
        else:
            self.waiting_reply = True if flag else False

    def __repr__(self):
        return '<User:%s(%s) waiting_reply=%s>' % (self.name, self.role, self.waiting_reply)


class Request(object):
    def __init__(self, request_id, cmd, params, source=None, ttl=600.0):
        self.request_id = request_id
        self.cmd = cmd
        self.params = params

        self.source = source
        self.target = None

        self.ttl = ttl
        self.delivered_at = None
        self.created_at = datetime.now()

    def reset(self):
        self.target = None
        self.delivered_at = None

    def __repr__(self):
        return '<Request:%s %s->%s %s>' % (
            self.cmd, self.source, self.target, self.params
        )


class SpiderServer(MessageQueue):
    def __init__(self, my_ident, port):
        super(SpiderServer, self).__init__(my_ident)
        self.listen(port)
        self.requests = {}
        self.users = {}

    # users funcs =================================
    def add_user(self, name):
        if not self.users.has_key(name):
            user = User(name)
            self.users[name] = user

    def del_user(self, name):
        if self.get_user(name):
            self.users.pop(name)

        for request in self.requests.values():
            if request.target == name:
                request.reset()

    def get_user(self, name):
        return self.users.get(name)

    def set_user_waiting(self, name, flag):
        user = self.get_user(name)
        if not user:
            return
        user.waiting(flag)

    def get_user_waiting(self, name):
        user = self.get_user(name)
        if not user:
            return None
        return user.waiting()

    def workers(self):
        return filter(lambda x: x.role == 'worker', self.users.values())

    def free_workers(self):
        return filter(lambda x: not x.waiting(), self.workers())

    # reqeusts funcs
    def free_requests(self):
        return [request for request in self.requests.values() if not request.target]

    def working_requests(self):
        return [request for request in self.requests.values() if request.target]

    # communicate funcs ================================
    def deliver_request(self, worker, request, ttl=600.0):
        request.delivered_at = datetime.now()
        request.target = worker
        request.ttl = ttl
        params = deepcopy(request.params)
        params["request_id"] = request.request_id
        self.send_command(worker, request.cmd, ttl=ttl, **params)

    def send_command(self, ident, cmd, ttl=600.0, **kwargs):
        kwargs["type"] = 'cmd'
        kwargs['cmd'] = cmd
        self.set_user_waiting(ident, True)
        self.send_json(ident, kwargs, ttl)

    def on_receive_json(self, ident, obj, mid64):
        log.debug('on_receive_json <- [%s]: %s %s', ident, repr(mid64), obj)
        msg_type = obj.get('type')

        if msg_type == 'result':
            result = deepcopy(obj)
            request_id = result['request_id']
            request = self.requests.pop(request_id) if self.requests.has_key(request_id) else None
            self.process_result(ident, result, request)
            self.set_user_waiting(ident, False)
        elif msg_type == 'request':
            params = deepcopy(obj)
            cmd = params.pop('cmd')
            request = Request(mid64, cmd, params, ident)
            self.requests[mid64] = request
            log.debug('request added: %s', request)
        else:
            log.warning('invalid msg type <- [%s]: %s %s', ident, obj, mid64)

    def process_result(self, worker, result, request):
        if result.has_key('error'):
            log.warning('cmd response error: [%s] %s', worker, result)
        if result.has_key('role'):
            return self._process_result_who(worker, result)

        if request.source:
            self.send_json(request.source, result)
        else:
            log.warning('unable to process result [%s]: result=%s, request=%s', worker, result, request)

    def _process_result_who(self, worker, response):
        user = self.get_user(worker)
        user.role = response.get('role')

    def on_loop(self):
        # 循环检查command是否timeout
        for request in self.working_requests():
            dt = datetime.now() - request.delivered_at
            if dt.seconds > request.ttl:
                request.reset()
                log.warning('request timeout: %s', request)

        # 分发 request
        free_requests = self.free_requests()
        if free_requests:
            free_workers = self.free_workers()
            random.shuffle(free_workers)
            for i in range(min(len(free_requests), len(free_workers))):
                worker = free_workers[i]
                request = free_requests[i]
                self.deliver_request(worker.name, request)

    def on_connect(self, conn, ident):
        self.add_user(ident)
        self.send_command(ident, 'who', 10.0)

    def on_disconnect(self, conn, ident):
        user = self.get_user(ident)
        log.info('disconnect [%s] (%s)', ident, user.role)
        self.del_user(ident)


if __name__ == '__main__':
    import thread
    server = SpiderServer('server', 44444)
#    server.loop()
    thread.start_new_thread(server.loop, ())

    while 1:
        time.sleep(10)