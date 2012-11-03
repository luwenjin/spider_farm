#coding:utf-8
import re
from copy import deepcopy

import requests
import pymongo
from bson import ObjectId, Binary
from pymongo.errors import OperationFailure
import thread
import time

from base import MessageQueue, to_zip, to_zip64, logging

log = logging.getLogger('spider')


class SpiderWorker(MessageQueue):
    def __init__(self, ident):
        super(SpiderWorker, self).__init__(ident)
        self.crawl_handler = CrawlHandler(self)

    def on_receive_json(self, caller, request, request_id):
        cmd = request.get('cmd')
        log.debug('receive cmd: %s', request)

        if cmd == 'crawl':
            result = self.crawl_handler.process(request)
        elif cmd == 'who':
            result = {'role': 'worker'}
        else:
            log.warning('invalid cmd: %s - %s', cmd, request)
            result = {'error': 'invalid cmd: %s' % cmd}
        if result:
            result['request_id'] = request.get('request_id')
            result['type'] = 'result'
            self.send_json(caller, result)


class CrawlHandler(object):
    optional_fields = [
        'params', 'data', 'headers', 'cookies', 'files', 'auth',
        'timeout','prefetch', 'cert', 'allow_redirects', 'proxies',
        'return_response', 'session', 'config', 'verify',
    ]
    def __init__(self, parent):
        self.parent = parent
        self.conns = {}
        self.dbs = {}

    def process(self, request):
        if not request.has_key('url'):
            return {'error': 'field required: url'}

        url = request.get('url')
        if url.startswith('http://'):
            kwargs = deepcopy(request)
            content = self.crawl(**kwargs)
            return {'status': 'ok', 'zip64': to_zip64(content)}
        elif url.startswith('mongodb://'):
            return self.process_mongo_url(request)
        else:
            return {'error': 'invalid url:' + url}

    def crawl(self, **kwargs):
        url = kwargs.get('url')
        if kwargs.has_key('method') and kwargs['method'].lower() in ['get', 'post']:
            method = kwargs['method'].lower()
        else:
            method = 'get'

        kwargs.setdefault('allow_redirects', True)
        kwargs.setdefault('timeout', 30.0)
        extra_keys = [key for key in kwargs if key not in self.optional_fields]
        for key in extra_keys:
            kwargs.pop(key)

        content = requests.request(method=method, url=url, **kwargs).content
        return content

    def process_mongo_url(self, request):
        kwargs = deepcopy(request)
        url = kwargs.get('url')
        pattern = r'mongodb://(:?(?P<username>.*?):(?P<password>.*?)@)?(?P<host>.*?):(?P<port>.*?)/(?P<db>.*?)/(?P<coll>.*?)/(?P<oid>.*)'

        m = re.match(pattern, url)
        if not m:
            return {'error': 'invalid url:' + url}
        args = m.groupdict()

        # conn
        host = str(args.get('host'))
        port = int(args.get('port', 27017))
        if not self.conns.has_key((host, port)):
            self.conns[(host,port)] = pymongo.Connection(host, port)
        conn = self.conns[(host,port)]

        # db
        db_name = args.get('db')
        if not db_name:
            return {'error': 'require db_name:' + url}
        if not self.dbs.has_key((host, port, db_name)):
            db = conn[db_name]
            username = args.get('username')
            password = args.get('password')
            if username and password:
                db.authenticate(str(username), str(password))
            self.dbs[(host, port, db_name)] = db
        db = self.dbs[(host, port, db_name)]

        # coll
        coll_name = str(args.get('coll'))
        coll = db[coll_name]

        # doc
        oid = str(args.get('oid'))
        if not ObjectId.is_valid(oid):
            return {'error': 'oid not valid:' + url}
        _id = ObjectId(oid)
        try:
            doc = coll.find_one(_id)
        except OperationFailure:
            self.dbs.pop((host, port, db_name))
            return {'error': 'auth failed:' + url}
        if not doc:
            return {'error': 'oid not found:' + url}

        content = self.crawl(**doc)
        coll.update({'_id': _id}, {'$set': {'zip': Binary(to_zip(content)), 'status': 'ok'}})
        return {'status': 'ok'}



if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    if len(args) != 2:
        print 'eg.: worker.py my_ident ip:port'
        args = ['spider6', 'getf5.com:44444']
    my_ident = args[0]
    ip, port = args[1].split(':')

    worker = SpiderWorker(my_ident)
    worker.connect(ip, int(port))

    thread.start_new_thread(worker.loop, ())

    while 1:
        time.sleep(10)



