from copy import deepcopy
import json
import re
from bson import ObjectId, Binary
from pymongo.errors import OperationFailure

import requests
import pymongo

from base import MessageQueue
from base import to_zip64, to_zip


class SpiderClient(MessageQueue):
    def __init__(self, ident):
        super(SpiderClient, self).__init__(ident)
        self.crawl_handler = CrawlHandler(self)

    def on_recv(self, conn, ident, message):
        try:
            obj = json.loads(message.data)
            self.process_message(ident, obj)
        except ValueError:
            print 'process_message exception:', message.data
            self.send_json(ident, {'status': 'error', 'info': 'unable_to_json_loads', 'request': message.data.decode('utf-8') })

    def process_message(self, caller, request):
        _type = request.get('type')
        if _type == 'crawl':
            self.crawl_handler.process(caller, request)
        else:
            print request


class CrawlHandler(object):
    optional_fields = [
        'params', 'data', 'headers', 'cookies', 'files', 'auth', 'timeout','prefetch', 'cert',
        'allow_redirects', 'proxies', 'return_response', 'session', 'config', 'verify',
    ]
    def __init__(self, parent):
        self.parent = parent
        self.conns = {}
        self.dbs = {}

    def process(self, caller, request):
        if not request.has_key('url'):
            return self.parent.send_json(caller,
                {'status': 'error', 'info': "field required: url", 'request': request})

        url = request.get('url')
        if url.startswith('http://'):
            kwargs = deepcopy(request)
            content = self.crawl(**kwargs)
            return self.parent.send_json(caller,
                {'status': 'ok', 'zip64': to_zip64(content), 'request': request})
        elif url.startswith('mongodb://'):
            return self.process_mongo_url(caller, request)
        else:
            return self.parent.send_json(caller,
                {'status': 'error', 'info': 'invalid url:' + url, 'request': request})

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
            return self.parent.send_json(caller, {"status": 'error', 'info': 'invalid url:' + url, 'request': request})
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
            return self.parent.send_json(caller, {"status": 'error', 'info': 'require db_name:' + url, 'request': request})
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
            return self.parent.send_json(caller, {"status": 'error', 'info': 'oid not valid:' + url, 'request': request})
        _id = ObjectId(oid)
        try:
            doc = coll.find_one(_id)
        except OperationFailure:
            self.dbs.pop((host, port, db_name))
            return self.parent.send_json(caller, {"status": 'error', 'info': 'auth failed:' + url, 'request': request})

        if not doc:
            return self.parent.send_json(caller, {"status": 'error', 'info': 'oid not found:' + url, 'request': request})

        content = self.crawl(**doc)
        coll.update({'_id': _id}, {'$set': {'zip': Binary(to_zip(content)), 'status': 'ok'}})
        return self.parent.send_json(caller, {'status': 'ok', 'json': request })



if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    if len(args) != 2:
        print 'eg.: spider.py my_ident ip:port'
    else:
        my_ident = args[0]
        ip, port = args[1].split(':')

        client = SpiderClient(my_ident)
        client.connect(ip, int(port))
        client.loop()



