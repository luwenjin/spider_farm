#coding:utf-8
import re
from copy import deepcopy

import requests
import pymongo
from bson import ObjectId, Binary
from pymongo.errors import OperationFailure

from base import MessageQueue, to_zip, to_zip64, logging, SRC_MID64

log = logging.getLogger('spider')


class SpiderWorker(MessageQueue):
    def __init__(self, ident):
        super(SpiderWorker, self).__init__(ident)
        self.crawl_handler = CrawlHandler(self)

    def on_receive_json(self, caller, request, mid64):
        _type = request.get('type')
        log.debug('receive cmd: %s', request)
        if _type == 'crawl':
            self.crawl_handler.process(caller, request)
        if _type == 'who':
            self.send_json(caller, {'role': 'worker', SRC_MID64: request[SRC_MID64]})
        else:
            log.warning('invalid cmd: %s - %s', _type, request)


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

    def process(self, caller, request):
        if not request.has_key('url'):
            return self.parent.send_json(caller,
                {'status': 'error', 'info': "field required: url", SRC_MID64: request[SRC_MID64]})

        url = request.get('url')
        if url.startswith('http://'):
            kwargs = deepcopy(request)
            content = self.crawl(**kwargs)
            return self.parent.send_json(caller,
                {'status': 'ok', 'zip64': to_zip64(content), SRC_MID64: request[SRC_MID64]})
        elif url.startswith('mongodb://'):
            return self.process_mongo_url(caller, request)
        else:
            return self.parent.send_json(caller,
                {'status': 'error', 'info': 'invalid url:' + url, SRC_MID64: request[SRC_MID64]})

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

    def process_mongo_url(self, caller, request):
        kwargs = deepcopy(request)
        url = kwargs.get('url')
        pattern = r'mongodb://(:?(?P<username>.*?):(?P<password>.*?)@)?(?P<host>.*?):(?P<port>.*?)/(?P<db>.*?)/(?P<coll>.*?)/(?P<oid>.*)'

        m = re.match(pattern, url)
        if not m:
            return self.parent.send_json(caller,
                {"status": 'error', 'info': 'invalid url:' + url, SRC_MID64: request[SRC_MID64]})
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
            return self.parent.send_json(caller,
                {"status": 'error', 'info': 'require db_name:' + url, SRC_MID64: request[SRC_MID64]})
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
            return self.parent.send_json(caller,
                {"status": 'error', 'info': 'oid not valid:' + url, SRC_MID64: request[SRC_MID64]})
        _id = ObjectId(oid)
        try:
            doc = coll.find_one(_id)
        except OperationFailure:
            self.dbs.pop((host, port, db_name))
            return self.parent.send_json(caller,
                {"status": 'error', 'info': 'auth failed:' + url, SRC_MID64: request[SRC_MID64]})

        if not doc:
            return self.parent.send_json(caller,
                {"status": 'error', 'info': 'oid not found:' + url, SRC_MID64: request[SRC_MID64]})

        content = self.crawl(**doc)
        coll.update({'_id': _id}, {'$set': {'zip': Binary(to_zip(content)), 'status': 'ok'}})
        return self.parent.send_json(caller, 
            {'status': 'ok',  SRC_MID64: request[SRC_MID64] })



if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    if len(args) != 2:
        print 'eg.: spider.py my_ident ip:port'
    else:
        my_ident = args[0]
        ip, port = args[1].split(':')

        client = SpiderWorker(my_ident)
        client.connect(ip, int(port))
        client.loop()



