"""
Copyright 2013 Russell Haering.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
from urllib.parse import urlencode
from dateutil.parser import parse as parse_datetime

from zope.interface import implementer

from twisted.internet import defer
from twisted.internet.ssl import optionsForClientTLS
from twisted.logger import Logger
from twisted.python.url import URL
from twisted.web.client import Agent, readBody, _requireSSL
from twisted.web.client import HTTPConnectionPool
from twisted.web.client import _HTTP11ClientFactory
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer, IPolicyForHTTPS

@implementer(IBodyProducer)
class StringProducer(object):
    def __init__(self, body):
        assert(isinstance(body, bytes))
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return defer.succeed(True)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass

class EtcdError(Exception):
    pass

class EtcdServerError(EtcdError):
    def __init__(self, **kwargs):
        super(EtcdServerError, self).__init__(kwargs)
        self.code = kwargs.get('errorCode')
        self.message = kwargs.get('message')
        self.cause = kwargs.get('cause')

class EtcdResponse(object):
    def __init__(self, action, node, index):
        self.action = action
        self.node = node
        self.index = index

class EtcdNode(object):
    directory = False

    def __init__(self, **kwargs):
        self.key = kwargs.get('key')

        if 'expiration' in kwargs:
            self.expiration = parse_datetime(kwargs['expiration'])
            
        else:
            self.expiration = None

    @staticmethod
    def from_response(**kwargs):
        if kwargs.get('dir', False):
            return EtcdDirectory(**kwargs)
        
        else:
            return EtcdFile(**kwargs)

class EtcdFile(EtcdNode):
    def __init__(self, **kwargs):
        super(EtcdFile, self).__init__(**kwargs)
        self.value = kwargs.get('value')
        self.modifiedIndex = int(kwargs.get('modifiedIndex'))
        self.createdIndex = int(kwargs.get('createdIndex'))


class EtcdDirectory(EtcdNode):
    directory = True

    def __init__(self, **kwargs):
        super(EtcdDirectory, self).__init__(**kwargs)
        self.modifiedIndex = int(kwargs.get('modifiedIndex'))
        self.createdIndex = int(kwargs.get('createdIndex'))
        if 'nodes' in kwargs:
            self.children = [EtcdNode.from_response(**obj) for obj in kwargs['nodes']]
        else:
            self.children = None


@implementer(IPolicyForHTTPS)
class PolicyForHTTPS(object):
    def __init__(self, ca, cert):
        self._ca = ca
        self._cert = cert

    @_requireSSL
    def creatorForNetloc(self, hostname, port):
        return optionsForClientTLS(hostname.decode("ascii"),
                trustRoot = self._ca, clientCertificate = self._cert)

class QuietHTTP11ClientFactory(_HTTP11ClientFactory):
    noisy = False

class EtcdClient(object):
    log = Logger()
    
    API_VERSION = 'v2'

    def __init__(self, reactor, node=('localhost', 2379), ca=None, cert=None):
        self.reactor = reactor
        self.node = node
        self.scheme = 'http'
        self.ca = ca
        self.cert = cert
        context = None
        if ca:
            self.scheme = 'https'
            context = PolicyForHTTPS(ca, cert)

        quietPool = HTTPConnectionPool(reactor, persistent = True)
        quietPool.maxPersistentPerHost = 2
        quietPool._factory = QuietHTTP11ClientFactory

        self.agent = Agent(self.reactor, contextFactory = context, pool = quietPool)

    def _decode_response(self, response):
        def decode(text):
            return json.loads(text.decode('utf-8'))
        d = readBody(response)
        d.addCallback(decode)
        d.addCallback(self._construct_response_object, response.headers)
        return d

    def _construct_response_object(self, obj, headers):
        if 'errorCode' in obj:
            raise EtcdServerError(**obj)
        index = int(headers.getRawHeaders(b'X-Etcd-Index', 0)[0])
        return EtcdResponse(obj['action'], EtcdNode.from_response(**obj['node']), index)

    def _format_kwarg(self, key, value):
        if isinstance(value, bool):
            if value:
                return 'true'
            else:
                return 'false'
        else:
            return value

    def _request(self, method, path, query = (), data = None, prefer_leader = False):
        url = URL(scheme = self.scheme,
                  host = self.node[0],
                  port = self.node[1],
                  path = path,
                  query = query)

        self.log.debug('Sending {m:} request to {u:}', m = method.decode('iso-8859-1'), u = url.asText())
        
        headers = None
        body = None

        if data:
            headers = Headers({b'Content-Type': [b'application/x-www-form-urlencoded; charset=utf-8']})
            body = StringProducer(data)

        return self.agent.request(method, url, headers, body)

    def _validate_key(self, key):
        if not key.startswith('/'):
            raise RuntimeError('keys must start with /')

    def _parse_key(self, key):
        self._validate_key(key)
        return key.split('/')[1:]

    def _build_path(self, key):
        path = [self.API_VERSION, 'keys']
        path.extend(self._parse_key(key))
        
    def _build_query(self, params, method_kwargs, param_map):
        for key in param_map.keys():
            if key in method_kwargs:
                params[param_map[key]] = self._format_kwarg(key, method_kwargs[key])

        return tuple(params.items())

    def _build_data(self, params, method_kwargs, param_map):
        for key in param_map.keys():
            if key in method_kwargs:
                params[param_map[key]] = self._format_kwarg(key, method_kwargs[key])

        return urlencode(params).encode('utf-8')
    
    def create(self, key, value, **kwargs):
        path = self._build_path(key)
        data = self._build_data({'value': value},
                                kwargs,
                                {
                                    'ttl': 'ttl',
                                })
        d = self._request(b'POST', path, data = data, prefer_leader = True)
        d.addCallback(self._decode_response)
        return d

    def set(self, key, **kwargs):
        path = self._build_path(key)
        data = self._build_params({},
                                  kwargs,
                                  {
                                      'ttl': 'ttl',
                                      'value': 'value',
                                      'prev_index': 'prevIndex',
                                      'prev_value': 'prevValue',
                                      'prev_exist': 'prevExist',
                                  })

        d = self._request(b'PUT', path, data = data, prefer_leader = True)
        d.addCallback(self._decode_response)
        return d

    def delete(self, key):
        path = self._build_path(key)
        d = self._request(b'DELETE', path, prefer_leader = True)
        d.addCallback(self._decode_response)
        return d

    def get(self, key, **kwargs):
        path = self._build_path(key)
        query = self._build_query({},
                                  kwargs,
                                  {
                                      'sorted': 'sorted',
                                      'recursive': 'recursive',
                                      'consistent': 'consistent',
                                      'wait': 'wait',
                                      'wait_index': 'waitIndex',
                                  })
        d = self._request(b'GET', path, query = query)
        d.addCallback(self._decode_response)
        return d
